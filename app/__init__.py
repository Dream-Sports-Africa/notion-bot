import os
import json
import flask
import redis
import oauth2client
import googleapiclient
from datetime import datetime
from notion.client import NotionClient
from gcsa.event import Event
from .sync_calendars import auth_flow, all_notion_tables, notion_base_url, notion_url, redis_client, sync_calendars_flask, GoogleCalendar


app = flask.Flask(__name__)

@app.route('/')
def landing_page():
    html = ''
    tracking_links = ''
    for table in all_notion_tables():
        b = table
        while hasattr(b, 'name') or (hasattr(b, 'title') and b.title == 'Tasks'):
            b = b.parent

        tracking_links += f'<li><a href="{notion_url(table.id)}">{b.title}</a></li>'

    html += '<head><title>DSA Notion Bot</title></head>'
    html += '<body>'
    html += '<h1>DSA Notion Bot 🤖</h1>'
    html += '<h2>This bot syncs pages in notion with your google calendar</h2>'
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
    html += '</body>'
    return html

@app.route('/google-callback')
def google_callback():
  flow = auth_flow(flask.url_for('google_callback', _external=True))

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
    return flask.Response(flask.stream_with_context(sync_calendars_flask(email)))

@app.route('/sync')
def sync_route():
    return flask.Response(flask.stream_with_context(sync_calendars_flask()))

@app.route('/add-notion-page', methods=['POST'])
def add_notion_page():
    notion_table_url = flask.request.form['notion_table_url']
    redis_client.lpush('notion-tables', notion_table_url)
    return flask.redirect('/')
