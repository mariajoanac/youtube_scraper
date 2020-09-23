#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# IMPORTS
from googleapiclient.discovery import build
import datetime
from youtube_dl import YoutubeDL
from apiclient.http import HttpError
import urllib
import time
import random
import argparse


# ===================== PERFORM SEARCHES =====================
# GET YOUTUBE VIDEO_IDS + METADATA FOR GIVEN KEYWORD

def get_video_ids_and_metadata(keyword,
                               developer_key,
                               date=datetime.datetime(1990, 1, 1, 0, 0),
                               time_delta_in_days=15000,
                               num_max_results=50):

    youtube_input = build("youtube", "v3", developerKey=developer_key)

    date_next = date + datetime.timedelta(days=time_delta_in_days)

    date = date.isoformat("T") + "Z"
    date_next = date_next.isoformat("T") + "Z"

    [video_ids, video_titles, channel_ids, descriptions] = youtube_search(search_string=keyword,
                                                                          youtube=youtube_input,
                                                                          date_after=date,
                                                                          date_before=date_next,
                                                                          max_results=num_max_results
                                                                          )

    return video_ids, video_titles, channel_ids, descriptions


def youtube_search(search_string,
                   developer_key,
                   date_after="1990-01-01T00:00:00Z",
                   date_before="2031-01-26T00:00:00Z",
                   max_results=50):

    # Call the search.list method to retrieve results matching the specified
    # query term.

    youtube = build("youtube", "v3", developerKey=developer_key)

    search_response = youtube.search().list(
        q=search_string,
        part="id,snippet",
        maxResults=50,
        publishedAfter=date_after,
        publishedBefore=date_before,
        relevanceLanguage="en",
        type="video"
    ).execute()

    video_ids = []
    video_titles = []
    channel_ids = []
    descriptions = []

    for results in search_response.get("items", []):
        video_ids.append("%s" % (results["id"]["videoId"]))
        video_titles.append("%s" % (results["snippet"]["title"]))
        channel_ids.append("%s" % (results["snippet"]["channelId"]))
        descriptions.append("%s" % (results["snippet"]["description"]))

    next_page_token = search_response.get('nextPageToken')

    while next_page_token:
        search_response = youtube.search().list(
            q=search_string,
            part="id,snippet",
            maxResults=50,
            pageToken=next_page_token,
            relevanceLanguage="en",
            type="video"
        ).execute()

        for results in search_response.get("items", []):
            video_ids.append("%s" % (results["id"]["videoId"]))
            video_titles.append("%s" % (results["snippet"]["title"]))
            channel_ids.append("%s" % (results["snippet"]["channelId"]))
            descriptions.append("%s" % (results["snippet"]["description"]))

        next_page_token = search_response.get('nextPageToken')

    return video_ids, video_titles, channel_ids, descriptions


# ================
# GET COMMENTS AND COMMENTERS FOR A VIDEO
def aux_get_comments_info_from_results(results,
                                       comments_text,
                                       comments_id,
                                       comments_user_id,
                                       comments_publish_date,
                                       comments_is_parent,
                                       developer_key):

    youtube = build("youtube", "v3", developerKey=developer_key)

    for item in results["items"]:
        try:
            text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
        except KeyError:
            text = ''
        try:
            comment_id = item["snippet"]["topLevelComment"]["id"]
        except KeyError:
            comment_id = ''
        try:
            user_id = item["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"]
        except KeyError:
            user_id = ''
        try:
            publish_date = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
        except KeyError:
            publish_date = ''
        try:
            comment_replies_count = item["snippet"]["totalReplyCount"]
        except KeyError:
            comment_replies_count = 0

        comments_text.append(text)
        comments_id.append(comment_id)
        comments_user_id.append(user_id)
        comments_publish_date.append(publish_date)
        comments_is_parent.append('Parent')

        if comment_replies_count > 0:
            try:
                results_comment_replies = youtube.comments().list(part="snippet", parentId=comment_id,
                                                                  textFormat="plainText", ).execute()
            except HttpError:
                results_comment_replies = {'items': []}

            for item in results_comment_replies["items"]:
                try:
                    text = item["snippet"]["textDisplay"]
                except KeyError:
                    text = ''
                try:
                    comment_id = item["id"]
                except KeyError:
                    comment_id = ''
                try:
                    user_id = item["snippet"]["authorChannelId"]["value"]
                except KeyError:
                    user_id = ''
                try:
                    publish_date = item["snippet"]["publishedAt"]
                except KeyError:
                    publish_date = ''

                comments_text.append(text)
                comments_id.append(comment_id)
                comments_user_id.append(user_id)
                comments_publish_date.append(publish_date)
                comments_is_parent.append('Child')

    return comments_text, comments_id, comments_user_id, comments_publish_date, comments_is_parent


def get_comments(video_id, developer_key):

    youtube = build("youtube", "v3", developerKey=developer_key)

    comments_text = []
    comments_id = []
    comments_user_id = []
    comments_publish_date = []
    comments_is_parent = []

    try:
        results = youtube.commentThreads().list(part="snippet", videoId=video_id, order="relevance",
                                                textFormat="plainText", ).execute()
    except HttpError:
        results = {'items': []}

    comments_text, comments_id, comments_user_id, comments_publish_date, comments_is_parent = aux_get_comments_info_from_results(
        results, comments_text, comments_id, comments_user_id, comments_publish_date, comments_is_parent)

    # Keep getting comments from the following pages
    num_comment_pages = 1
    max_comments_pages = 10
    while ("nextPageToken" in results) and num_comment_pages <= max_comments_pages:
        num_comment_pages += 1
        try:
            results = youtube.commentThreads().list(part="snippet", videoId=video_id,
                                                    pageToken=results["nextPageToken"], order="relevance",
                                                    textFormat="plainText").execute()
        except HttpError:
            results = {'items': []}

        comments_text, comments_id, comments_user_id, comments_publish_date, comments_is_parent = aux_get_comments_info_from_results(
            results, comments_text, comments_id, comments_user_id, comments_publish_date, comments_is_parent)

    return comments_text, comments_id, comments_user_id, comments_publish_date, comments_is_parent


# ================
# GET VIDEO'S RECOMENDED VIDEOS + METADATA
def extend_results(video_id, developer_key):

    youtube = build("youtube", "v3", developerKey=developer_key)

    videos_id = []
    videos_title = []
    videos_description = []
    channel_ids = []

    results = youtube.search().list(
        part="id,snippet",
        type="video",
        relatedToVideoId=video_id
    ).execute()

    for item in results["items"]:
        try:
            video_id = item["id"]["videoId"]
        except KeyError:
            video_id = []
        try:
            video_title = item["snippet"]["title"]
        except KeyError:
            video_title = []
        try:
            video_description = item["snippet"]["description"]
        except KeyError:
            video_description = []
        try:
            channel_id = item["snippet"]["channelId"]
        except KeyError:
            channel_id = []

        videos_id.append(video_id)
        videos_title.append(video_title)
        videos_description.append(video_description)
        channel_ids.append(channel_id)

    extended_results = zip(videos_id, videos_title, videos_description, channel_ids)

    return extended_results


def video_get_details(video_id,
                      developer_key,
                      video_title='title',
                      video_description='description',
                      video_uploader_channel_id='uploader channel id',
                      out_dir='.'):

    youtube = build("youtube", "v3", developerKey=developer_key)

    print(video_id)

    video_url = 'https://www.youtube.com/watch?v=' + video_id
    # video_url = unicode(video_url, "utf-8")

    # make request
    time.sleep(int(random.random() * 10))
    ydl_opts = {'writesubtitles': True,
                'writeautomaticsub': True,
                'quiet': True,
                'no_warnings': True}

    bool_request_error = True

    while bool_request_error:
        try:
            with YoutubeDL(ydl_opts) as ydl:
                meta = ydl.extract_info(video_url, download=False)
            bool_request_error = False
        except:
            time.sleep(int(120))
            bool_request_error = True

    # if request failed wait and request again
    while meta['uploader_id'] is None:
        print
        'request failed... retrying'
        time.sleep(int(random.random() * 10 + 20))
        ydl_opts = {'writesubtitles': True,
                    'writeautomaticsub': True,
                    'quiet': True,
                    'no_warnings': True}
        with YoutubeDL(ydl_opts) as ydl:
            meta = ydl.extract_info(video_url, download=False)

    # get metadata
    video_uploader_channel_id = meta['uploader_id'].replace("\n", " ").encode('utf-8')
    video_title = meta['title'].replace("\n", " ").encode('utf-8')
    video_description = meta['description'].replace("\n", " ").encode('utf-8')
    video_category = ', '.join(meta['categories']).replace("\n", " ").encode('utf-8')
    video_like_count = str(meta['like_count']).encode('utf-8')
    video_dislike_count = str(meta['dislike_count']).encode('utf-8')
    video_duration = str(meta['duration']).encode('utf-8')
    video_tags = ', '.join(meta['tags']).replace("\n", " ").encode('utf-8')
    video_view_count = str(meta['view_count']).encode('utf-8')
    video_upload_date = str(meta['upload_date']).encode('utf-8')

    # get subtitles/captions if they exist
    try:
        captions_url = meta['automatic_captions']['en'][0]['url']
        url_response = urllib.urlopen(captions_url)
        captions = url_response.read()
    except KeyError:
        captions = ''

    # get extended results
    extended_results = extend_results(video_id, youtube)

    # get comments
    comments_text, comments_id, comments_user_id, comments_publish_date, comments_is_parent = get_comments(video_id,
                                                                                                           youtube)

    # save metadata, comments, and captions

    filename = out_dir + '/' + video_upload_date + '_' + video_id + '.txt'

    f = open(filename, 'w')

    f.write('\n' + '##METADATA##' + '\n\n')

    f.write(video_id + '\n')
    f.write(video_uploader_channel_id + '\n')
    f.write(video_title + '\n')
    f.write(video_description + '\n')
    f.write(video_category + '\n')
    f.write(video_like_count + '\n')
    f.write(video_dislike_count + '\n')
    f.write(video_duration + '\n')
    f.write(video_tags + '\n')
    f.write(video_view_count + '\n')
    f.write(video_upload_date + '\n')

    for extended_result in extended_results:
        extended_video_id, extended_video_title, extended_video_description, extended_channel_id = extended_result
        f.write(extended_video_id.encode('utf-8') + '\t')
    f.write('\n')

    f.write('\n' + '##COMMENTS##' + '\n\n')

    for comment_text, comment_id, comment_user_id, comment_publish_date, comment_is_parent in zip(comments_text,
                                                                                                  comments_id,
                                                                                                  comments_user_id,
                                                                                                  comments_publish_date,
                                                                                                  comments_is_parent):
        f.write(comment_user_id.encode('utf-8') + '\t')
        f.write(comment_publish_date.encode('utf-8') + '\t')
        f.write(comment_is_parent.encode('utf-8') + '\t')
        f.write(comment_text.replace("\n", " ").encode('utf-8') + '\n')

    f.write('\n')
    f.write('\n' + '##CAPTIONS##' + '\n\n')

    f.write(captions + '\n')

    f.close

    # download thumbnail and save as video_id.png

    time.sleep(int(random.random() * 10))

    filename = out_dir + '/' + video_upload_date + '_' + video_id + '.jpg'
    url = 'https://img.youtube.com/vi/' + video_id + '/hqdefault.jpg'
    urllib.urlretrieve(url, filename)

    return


def main():

    parser = argparse.ArgumentParser(description='Scrape up to 50 YouTube videos + metadata from keyword from start_date until end_date')
    parser.add_argument('--keyword', type=str, default='news', help='keyword to search on YouTube')
    parser.add_argument('--developer_key', type=str, default='', help='individual developer key for youtube API')
    parser.add_argument('--date_start', type=str, default='1900-01-01', help='begin data for search in format YYYY-mm-dd')
    parser.add_argument('--date_end', type=str, default='2021-01-01', help='end data for search in format YYYY-mm-dd')
    parser.add_argument('--out_dir', type=str, default='.', help='directory in which to save the scraped data')

    keyword = argparse.keyword
    developer_key = argparse.developer_key
    date_start = argparse.date_start + "T00:00:00Z"
    date_end = argparse.date_end + "T00:00:00Z"
    out_dir = argparse.out_dir

    video_ids, video_titles, channel_ids, descriptions = youtube_search(keyword,
                                                                        developer_key,
                                                                        date_after=date_start,
                                                                        date_before=date_end
                                                                        )
    for video_id in video_ids:
        video_get_details(video_id, developer_key, out_dir=out_dir)

    return

if __name__ == "__main__":
    main()
