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

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

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

def all_notion_pages():
    for table in all_notion_tables():
        for page in table.default_query().execute():
            yield page

def all_notion_pages_with_assignees_and_due_dates():
    for page in all_notion_pages():
        if hasattr(page, 'due') and page.due and hasattr(page, 'assign') and page.assign:
            yield page

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

    info_the_same = page.due.start == page_in_redis['start'] and page.due.end == page_in_redis['end'] and page.title == page_in_redis['summary']

    past_assignees = [a['email'] for a in page_in_redis["added"]]
    current_assignees = [user.email for user in page.assign if user.email in calendars]

    if info_the_same and past_assignees == current_assignees:
        print("  - No change ")
        return

    all_assignees = set(past_assignees + current_assignees)

    next_added = []

    for email in all_assignees:
        if not email in current_assignees:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    calendars[email].delete_event(Event(event_id = added['event_id']))
                    print(f'  - removed from calendar for {email}')
        elif not email in past_assignees:
            next_added += add_events(calendars, event, [email])
            print(f'  - added to calendar for {email}')
        else:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    next_added += [added]
                    if not info_the_same:
                        event_with_id = event_from_page(page, event_id = added['event_id'])
                        calendars[email].update_event(event_with_id)
                        print(f'  - edited event details for {email}')
                    else:
                        print("  - nothing to update")

    redis_set_notion_page(page, next_added)

def sync_calendars():
    print("Syncing calendars...")
    calendars = {}
    for email in redis_keys('creds'):
        credentials = oauth2client.client.OAuth2Credentials.from_json(redis_json_get(f'creds:{email}'))
        calendars[email] = GoogleCalendar(credentials)

    notion_page_ids = redis_keys('notion-page')

    for page in all_notion_pages_with_assignees_and_due_dates():
        if page.id in notion_page_ids:
            print(f'Page {page.id} "{page.title}" already on peoples calendars, syncing...')
            sync_events(calendars, page)
        else:
            emails = assignees(calendars, page)
            if emails:
                print(f'New page {page.id} "{page.title}", adding for {emails}...')
                added = add_events(calendars, event_from_page(page), emails)
                redis_set_notion_page(page, added)

    print("Calendars synced...")

if __name__ == "__main__":
    sync_calendars()
