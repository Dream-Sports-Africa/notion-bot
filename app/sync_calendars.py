import os
import json
import redis
import dotenv
import oauth2client
import googleapiclient
from datetime import datetime
from notion.client import NotionClient
from gcsa.google_calendar import GoogleCalendar
from gcsa.event import Event

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

credential_file_path = '/tmp/credentials.json'

notion_client = NotionClient(token_v2 = os.environ['NOTION_TOKEN'])

notion_base_url = os.environ['NOTION_BASE_URL']

def notion_url(id):
    return f'{notion_base_url}/{id}'

with open(credential_file_path, 'w') as outfile:
    json.dump({
        "web": {
            "client_id": os.environ['GOOGLE_CALENDAR_CLIENT_ID'],
            "project_id": os.environ['GOOGLE_CALENDAR_PROJECT_ID'],
            "client_secret": os.environ['GOOGLE_CALENDAR_CLIENT_SECRET'],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": [os.environ['GOOGLE_CALENDAR_REDIRECT_URI']]
        }
    }, outfile)

# gcsa's GoogleCalendar.__init__ function assumes you want to store credentials locally using pickle,
# so we monkey patch that behavior here
def google_calendar_init(self, credentials):
    self.calendar = 'primary'
    self.credentials = credentials
    self.service = googleapiclient.discovery.build('calendar', 'v3', credentials=credentials)

GoogleCalendar.__init__ = google_calendar_init

# Connect to redis and ping to ensure the connection works
redis_client = redis.from_url(os.environ['REDIS_URL']) if os.environ.get('REDIS_URL') else redis.Redis(host='localhost', port=6379, db=0)
redis_client.ping()

def all_notion_tables():
    for t in redis_client.lrange('notion-tables', 0, -1):
        yield notion_client.get_collection_view(t.decode("utf-8"))

def pretty_table_name(table):
    b = table
    while hasattr(b, 'name') or (hasattr(b, 'title') and b.title == 'Tasks'):
        b = b.parent
    return b.title

def all_notion_pages(table):
    return table.default_query().execute()

def redis_keys(prefix):
    return set(key.decode("utf-8").replace(f'{prefix}:', '') for key in redis_client.keys(f'{prefix}:*'))

def redis_json_get(key):
    return json.loads(redis_client.get(key))

def event_from_page(page, event_id = None):
    return Event(summary = page.title, description = notion_url(page.id), start = page.due.start, end = page.due.end, event_id = event_id)

def assignees(calendars, page):
    return set(user.email for user in page.assign if user.email in calendars)

def add_events(calendars, event, emails):
    return [{ "email": email, "event_id": calendars[email].add_event(event).id } for email in emails]

def redis_set_notion_page(page, added):
    key = f'notion-page:{page.id}'
    if not added:
        redis_client.delete(key)
    else:
        event = event_from_page(page)
        redis_client.set(key, json.dumps({
            "summary": event.summary,
            "description": event.description,
            "start": event.start.isoformat(),
            "end": event.end.isoformat(),
            "added": added
        }))

def auth_flow(redirect_uri):
    return oauth2client.client.flow_from_clientsecrets(
        credential_file_path,
        scope='https://www.googleapis.com/auth/calendar',
        redirect_uri=redirect_uri)

def sync_events(calendars, page):
    event = event_from_page(page)

    page_in_redis = redis_json_get(f'notion-page:{page.id}')
    page_in_redis['start'] = datetime.fromisoformat(page_in_redis['start'])
    page_in_redis['end'] = datetime.fromisoformat(page_in_redis['end'])

    info_the_same = event.start == page_in_redis['start'] and event.end == page_in_redis['end'] and event.summary == page_in_redis['summary']

    past_assignees = [a['email'] for a in page_in_redis["added"]]
    current_assignees = [user.email for user in page.assign if user.email in calendars]

    if info_the_same and past_assignees == current_assignees:
        yield { "action": "no_changes", "emails": current_assignees }
        return

    all_assignees = set(past_assignees + current_assignees)

    next_added = []

    for email in all_assignees:
        if not email in current_assignees:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    calendars[email].delete_event(Event(summary = event.summary, start = event.start, event_id = added['event_id']))
                    yield { "action": "delete_event", "email": email }
        elif not email in past_assignees:
            next_added += add_events(calendars, event, [email])
            yield { "action": "add_event", "email": email }
        else:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    next_added += [added]
                    if not info_the_same:
                        event_with_id = event_from_page(page, event_id = added['event_id'])
                        calendars[email].update_event(event_with_id)
                        yield { "action": "update_event", "email": email }
                    else:
                        yield { "action": "no_change", "email": email }

    redis_set_notion_page(page, next_added)

def sync_calendars():
    yield { "action": "sync_calendars_start" }

    calendars = {}
    for email in redis_keys('creds'):
        credentials = oauth2client.client.OAuth2Credentials.from_json(redis_json_get(f'creds:{email}'))
        calendars[email] = GoogleCalendar(credentials)

    notion_page_ids = redis_keys('notion-page')

    for table in all_notion_tables():
        yield {
            "action": "sync_table_start",
            "table_url": notion_url(table.id),
            "table_name": pretty_table_name(table)
        }

        for page in all_notion_pages(table):
            due = getattr(page, 'due', None)
            assign = getattr(page, 'assign', None)
            if not due:
                yield {
                    "action": "skip_page_no_due_date",
                    "page_id": page.id,
                    "page_title": page.title
                }
            elif not assign:
                yield {
                    "action": "skip_page_no_assign",
                    "page_id": page.id,
                    "page_title": page.title
                }
            elif page.id in notion_page_ids:
                yield {
                    "action": "sync_events_start",
                    "page_id": page.id,
                    "page_title": page.title
                }
                for message in sync_events(calendars, page):
                    yield message
            else:
                emails = assignees(calendars, page)
                if emails:
                    yield {
                        "action": "add_events",
                        "page_id": page.id,
                        "page_title": page.title,
                        "emails": emails
                    }
                    # yield f'New page {page.id} "{page.title}", adding for {emails}...'
                    added = add_events(calendars, event_from_page(page), emails)
                    redis_set_notion_page(page, added)

    yield { "action": "sync_calendars_finished" }

def mailto_link(email):
    return f'<a href="mailto:{email}">{email}</a>'

def mailto_links(emails):
    return ", ".join(mailto_link(email) for email in emails)

def sync_calendars_flask(email = None):
    if email:
        yield f'<p>DSA Notion Bot now authenticated to post to calendar for {email}.</p>'

    for event in sync_calendars():
        if event['action'] == 'sync_calendars_start':
            yield '<p>Syncing calendars...</p>'
        elif event['action'] == 'sync_table_start':
            yield f'<p>Syncing pages for <a target="_blank" href="{event["table_url"]}">{event["table_name"]}</a>...</p>'
        elif event['action'] == 'skip_page_no_due_date':
            yield f'<p>Skipping sync of <a target="_blank" href={notion_url(event["page_id"])}>{event["page_title"]}</a> as it has no due date...</p>'
        elif event['action'] == 'skip_page_no_assign':
            yield f'<p>Skipping sync of <a target="_blank" href={notion_url(event["page_id"])}>{event["page_title"]}</a> as no one is assigned to it...</p>'
        elif event['action'] == 'sync_events_start':
            yield f'<p>Syncing events for <a target="_blank" href={notion_url(event["page_id"])}>{event["page_title"]}</a>...</p>'
        elif event['action'] == 'add_events':
            yield f'<p>Adding events for <a target="_blank" href={notion_url(event["page_id"])}>{event["page_title"]}</a> to calendars of {mailto_links(event["emails"])}...</p>'
        elif event['action'] == 'sync_calendars_finished':
            yield '<p>Syncing calendars finished!</p>'
        elif event['action'] == 'no_changes':
            yield f'<p>&emsp;Calendar info already up to date for {mailto_links(event["emails"])}</p>'
        elif event['action'] == 'delete_event':
            yield f'<p>&emsp;Removed event from calendar of {mailto_link(event["email"])}</p>'
        elif event['action'] == 'add_event':
            yield f'<p>&emsp;Added event to calendar of {mailto_link(event["email"])}</p>'
        elif event['action'] == 'update_event':
            yield f'<p>&emsp;Updated event details for calendar of {mailto_link(event["email"])}</p>'
        elif event['action'] == 'no_change':
            yield f'<p>&emsp;No changes necessary to event details for calendar of {mailto_link(event["email"])}</p>'
        else:
            raise f'Unknown action {event["action"]}'

if __name__ == "__main__":
    for message in sync_calendars():
        print(message)
