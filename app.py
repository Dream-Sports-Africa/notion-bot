import os
import json
import flask
import redis
import dotenv
import oauth2client
import googleapiclient
from datetime import datetime
from notion.client import NotionClient
from gcsa.google_calendar import GoogleCalendar
from gcsa.event import Event

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

app = flask.Flask(__name__)

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
        if page.due and page.assign:
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

def sync_events(calendars, page):
    event = event_from_page(page)

    page_in_redis = redis_json_get(f'notion-page:{page.id}')
    page_in_redis['start'] = datetime.fromisoformat(page_in_redis['start'])
    page_in_redis['end'] = datetime.fromisoformat(page_in_redis['end'])

    info_the_same = page.due.start == page_in_redis['start'] and page.due.end == page_in_redis['end'] and page.title == page_in_redis['summary']

    past_assignees = [a['email'] for a in page_in_redis["added"]]
    current_assignees = [user.email for user in page.assign if user.email in calendars]

    if info_the_same and past_assignees == current_assignees:
        return

    all_assignees = set(past_assignees + current_assignees)

    next_added = []

    for email in all_assignees:
        if not email in current_assignees:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    calendars[email].delete_event(Event(event_id = added['event_id']))
        elif not email in past_assignees:
            next_added += add_events(calendars, event, [email])
        else:
            for added in page_in_redis["added"]:
                if added['email'] == email:
                    next_added += [added]
                    if not info_the_same:
                        event_with_id = event_from_page(page, event_id = added['event_id'])
                        calendars[email].update_event(event_with_id)

    redis_set_notion_page(page, next_added)

def sync_calendars(emails = None):
    if not emails:
        emails = redis_keys('creds')

    calendars = {}
    for email in emails:
        credentials = oauth2client.client.OAuth2Credentials.from_json(redis_json_get(f'creds:{email}'))
        calendars[email] = GoogleCalendar(credentials)

    notion_page_ids = redis_keys('notion-page')

    for page in all_notion_pages_with_assignees_and_due_dates():
        if page.id in notion_page_ids:
            sync_events(calendars, page)
        else:
            emails = assignees(calendars, page)
            if emails:
                added = add_events(calendars, event_from_page(page), emails)
                redis_set_notion_page(page, added)


@app.route('/')
def landing_page():
    html = ''
    tracking_links = ''
    for table in all_notion_tables():
        b = table
        while hasattr(b, 'name') or (hasattr(b, 'title') and b.title == 'Tasks'):
            b = b.parent

        tracking_links += f'<li><a href="{notion_url(table.id)}">{b.title}</a></li>'

    html += '<a href="/google-callback">Set up calendar sync</a>'
    html += '<br>'
    html += '<a href="/sync">Force calendar sync</a>'
    html += '<br><br>'
    html += '<span>Tracking the following tables:</span>'
    html += f'<ul>{tracking_links}</ul>'
    html += '<form action="/add-notion-page" method="post">'
    html += '    <label for="notion_table_url">Notion Table URL</label><br>'
    html += f'    <input name="notion_table_url" type="url" placeholder="{notion_base_url}" required><br>'
    html += '    <button type="submit">'
    html += '        Add Notion Table to Track'
    html += '    </button>'
    html += '</form>'
    return html

@app.route('/google-callback')
def google_callback():
  flow = oauth2client.client.flow_from_clientsecrets(
      credential_file_path,
      scope='https://www.googleapis.com/auth/calendar',
      redirect_uri=flask.url_for('google_callback', _external=True))

  if 'code' not in flask.request.args:
    auth_uri = flow.step1_get_authorize_url()
    return flask.redirect(auth_uri)
  else:
    auth_code = flask.request.args.get('code')
    credentials = flow.step2_exchange(auth_code)

    calendar = GoogleCalendar(credentials)
    primary_email = calendar.service.calendars().get(calendarId = 'primary').execute()['id']
    json_creds = credentials.to_json()

    redis_client.set(f'creds:{primary_email}', json.dumps(json_creds))
    return flask.redirect(f'{flask.url_for("finishoauth")}?email={primary_email}')

@app.route('/finishoauth')
def finishoauth():
    email = flask.request.args.get('email', default = '*', type = str)
    sync_calendars(set([email]))
    return f'<p>Calendar sync set up for {email}</p><br><a href="/sync?email={email}">Force calendar sync</a>'

@app.route('/sync')
def sync_route():
    email = flask.request.args.get('email', default = '*', type = str)
    if email == '*':
        sync_calendars()
        return '<p>Calendars synced for all users</p>'
    else:
        sync_calendars([email])
        return f'<p>Calendar synced for {email}</p>'

@app.route('/add-notion-page', methods=['POST'])
def add_notion_page():
    notion_table_url = flask.request.form['notion_table_url']
    redis_client.lpush('notion-tables', notion_table_url)
    return flask.redirect('/')
