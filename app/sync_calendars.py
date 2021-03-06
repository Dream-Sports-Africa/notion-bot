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

notion_client = NotionClient(token_v2 = os.environ['NOTION_TOKEN'])

notion_base_url = os.environ['NOTION_BASE_URL']

def notion_url(id):
    return f'{notion_base_url}/{id}'

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

def redis_page_get(page_id):
    page_in_redis = redis_json_get(f'notion-page:{page_id}')
    page_in_redis['start'] = datetime.fromisoformat(page_in_redis['start'])
    page_in_redis['end'] = datetime.fromisoformat(page_in_redis['end'])
    return page_in_redis

def event_from_page(page, event_id = None):
    return Event(summary = page.title, description = notion_url(page.id), start = page.due.start, end = page.due.end, event_id = event_id)

def assignees(page):
    return set(user.email for user in page.assign)

def add_event(calendars, event, email):
    try:
        return { "email": email, "event_id": calendars[email].add_event(event).id }
    except Exception as e:
        print(e)

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
    return oauth2client.client.OAuth2WebServerFlow(
        client_id=os.environ['GOOGLE_CALENDAR_CLIENT_ID'],
        client_secret=os.environ['GOOGLE_CALENDAR_CLIENT_SECRET'],
        scope='https://www.googleapis.com/auth/calendar',
        redirect_uri=redirect_uri,
        prompt='consent')

def sync_events(calendars, page):
    event = event_from_page(page)

    page_in_redis = redis_page_get(page.id)

    info_the_same = event.start == page_in_redis['start'] and event.end == page_in_redis['end'] and event.summary == page_in_redis['summary']

    past_assignees = [a['email'] for a in page_in_redis["added"]]
    current_assignees = [user.email for user in page.assign if user.email in calendars]

    if info_the_same and past_assignees == current_assignees:
        yield { "action": "no_changes", "emails": current_assignees }
        return

    all_assignees = set(past_assignees + current_assignees)

    next_added = []

    for email in all_assignees:
        if not email in calendars:
            yield { "action": "skip_no_email", "email": email }
        if not email in current_assignees:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    yield { "action": "delete_event", "email": email }
                    try:
                        calendars[email].delete_event(Event(summary = event.summary, start = event.start, event_id = added['event_id']))
                    except Exception as e:
                        print(e)

        elif not email in past_assignees:
            next_added += [add_event(calendars, event, email)]
            yield { "action": "add_to_calendar", "email": email }
        else:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    next_added += [added]
                    if not info_the_same:
                        event_with_id = event_from_page(page, event_id = added['event_id'])
                        yield { "action": "update_event", "email": email }
                        try:
                            calendars[email].update_event(event_with_id)
                        except Exception as e:
                            print(e)
                    else:
                        yield { "action": "no_change", "email": email }

    redis_set_notion_page(page, next_added)

def get_calendars():
    calendars = {}
    for email in redis_keys('creds'):
        credentials = oauth2client.client.OAuth2Credentials.from_json(redis_json_get(f'creds:{email}'))
        calendars[email] = GoogleCalendar(credentials)
    return calendars

def sync_calendars():
    yield { "action": "sync_calendars_start" }

    calendars = get_calendars()

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
                emails = assignees(page)
                if emails:
                    added = []
                    for email in emails:
                        if email in calendars:
                            yield {
                                "action": "add_event",
                                "page_id": page.id,
                                "page_title": page.title,
                                "email": email
                            }
                            added += [add_event(calendars, event_from_page(page), email)]
                        else:
                            yield {
                                "action": "skip_event",
                                "page_id": page.id,
                                "page_title": page.title,
                                "email": email
                            }

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
        print(event)
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
        elif event['action'] == 'add_event':
            yield f'<p>Adding event for <a target="_blank" href={notion_url(event["page_id"])}>{event["page_title"]}</a> to calendar of {mailto_link(event["email"])}...</p>'
        elif event['action'] == 'skip_event':
            yield f'<p>Not adding event for <a target="_blank" href={notion_url(event["page_id"])}>{event["page_title"]}</a> to calendar of {mailto_link(event["email"])}, because we don\'t have permission...</p>'
        elif event['action'] == 'sync_calendars_finished':
            yield '<p>Syncing calendars finished!</p>'
        elif event['action'] == 'no_changes':
            yield f'<p>&emsp;Calendar info already up to date for {mailto_links(event["emails"])}</p>'
        elif event['action'] == 'skip_no_email':
            yield f'<p>&emsp;Cannot sync with calendar of {mailto_link(event["email"])} as permission not yet given</p>'
        elif event['action'] == 'delete_event':
            yield f'<p>&emsp;Removed event from calendar of {mailto_link(event["email"])}</p>'
        elif event['action'] == 'add_to_calendar':
            yield f'<p>&emsp;Added event to calendar of {mailto_link(event["email"])}</p>'
        elif event['action'] == 'update_event':
            yield f'<p>&emsp;Updated event details for calendar of {mailto_link(event["email"])}</p>'
        elif event['action'] == 'no_change':
            yield f'<p>&emsp;No changes necessary to event details for calendar of {mailto_link(event["email"])}</p>'
        else:
            raise f'Unknown action {event["action"]}'

def flush_events_and_creds():
    calendars = get_calendars()

    notion_page_ids = redis_keys('notion-page')

    for page_id in notion_page_ids:
        page_in_redis = redis_page_get(page_id)

        for added in page_in_redis["added"]:
            email = added['email']
            try:
                calendars[email].delete_event(Event(summary = page_in_redis['summary'], start = page_in_redis['start'], event_id = added['event_id']))
            except Exception as e:
                print(e)

        redis_client.delete(f'notion-page:{page_id}')

    for email in redis_keys('creds'):
        redis_client.delete(f'creds:{email}')

if __name__ == "__main__":
    for message in sync_calendars():
        print(message)
