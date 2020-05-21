import datetime
import os
import random
import threading
import time
from functools import partial

import inflect
import praw
import schedule
from dotenv import load_dotenv
from loguru import logger
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sqlitedict import SqliteDict

load_dotenv()

sia = SentimentIntensityAnalyzer()
p = inflect.engine()

logger.add("cday_bot.log", level="INFO")

db = SqliteDict("cakedays.sqlite", autocommit=True)

reddit = praw.Reddit(
    user_agent=os.environ["CADABOT_USER_AGENT"],
    client_id=os.environ["CADABOT_CLIENT_ID"],
    client_secret=os.environ["CADABOT_CLIENT_SECRET"],
    username=os.environ["CADABOT_USERNAME"],
    password=os.environ["CADABOT_PASSWORD"],
)


def choose_cakeday_wish(age_str):
    emojis = ["😄", "😃", "🍰"]
    emoji = random.choice(emojis)
    return f"Happy {age_str} cake day! {emoji}"


def submissions_and_comments(subreddit, **kwargs):
    results = []
    results.extend(subreddit.new(**kwargs))
    results.extend(subreddit.comments(**kwargs))
    results.sort(key=lambda post: post.created_utc, reverse=True)
    return results


def get_cakeday_status(redditor):
    username = redditor.name

    user = db.get(username, None)

    if user:
        assert redditor.created_utc == user["created_utc"]
        return user

    created_utc = redditor.created_utc
    db[username] = {"created_utc": created_utc, "years_wished_cakeday": []}
    return db[username]


def remove_old_cakedays():
    logger.info("Clearing out old entries in sqlite")
    global db
    logger.info(f"Currently {len(db)} entries")
    to_remove = []
    tnow = datetime.datetime.utcnow()
    today_dm = (tnow.day, tnow.month)

    # For all keys / values in the database.
    for k, v in db.items():
        # Get the time created.
        t_created = datetime.datetime.utcfromtimestamp(v["created_utc"])
        # Get the day and month created.
        created_dm = (t_created.day, t_created.month)
        # If the user wasn't created today, we have no need to track them.
        if created_dm != today_dm:
            to_remove.append(k)

    for k in to_remove:
        del db[k]

    logger.info(f"{len(db)} entries remaining after purge")


def post_if_cakeday(post, tnow):
    author = post.author
    status = get_cakeday_status(author)

    # If we've already wished the user a Happy Cakeday, return.
    if tnow.year in status["years_wished_cakeday"]:
        return

    # Get the user's cake day.
    cakeday = datetime.datetime.utcfromtimestamp(status["created_utc"])

    # If the cake day year is the same as this year, then it can't be an
    # anniversary.
    if cakeday.year == tnow.year:
        return

    # Get the date of their cakeday this year.
    cakeday_this_year = cakeday.replace(year=tnow.year)

    # If the dates match, their cake day is today.
    if cakeday_this_year.date() == tnow.date():
        logger.info(f"Found cake day for u/{author.name} ({cakeday})")

        # Calculate their age.
        age = tnow.year - cakeday.year
        # Generate a response string.
        age_ordinal = p.ordinal(age)
        age_str = f"{age_ordinal[:-2]}^{age_ordinal[-2:]}"
        text = choose_cakeday_wish(age_str)
        # Post the response.
        response = post.reply(text)
        # Record that we've posted a response.
        status["years_wished_cakeday"].append(tnow.year)
        db[author.name] = status

        logger.info(f"Link: https://www.reddit.com{response.permalink}")


def post_if_response_to_cakeday_wish(post, tnow):
    # Get the author's cake day, and check that it is still their cake day
    # (to make sure that the "You're welcome" messages are still relevant)
    status = get_cakeday_status(post.author)
    cakeday = datetime.datetime.utcfromtimestamp(status["created_utc"])
    cakeday_this_year = cakeday.replace(year=tnow.year)

    if cakeday_this_year.date() != tnow.date():
        return

    # If the post is not a comment, return.
    if not isinstance(post, praw.models.Comment):
        return

    # If the post is not a thank you post, return.
    if not "thank" in post.body.lower():
        return

    polarity = sia.polarity_scores(post.body)

    # If the message is not positive or contains negativity, return.
    if not (polarity["pos"] > 0.4 and polarity["neg"] == 0):
        return

    # Get the parent of the post and see if it was a CaDaBot comment.
    parent = post.parent()
    if not parent.author == "CaDaBot":
        return

    # Check the post came from the person CaDaBot replied to.
    if not parent.parent().author == post.author:
        return

    # Check that we've not already replied to them.
    post.refresh()
    for reply in post.replies:
        if reply.author == "CaDaBot":
            return

    options = [
        "You're welcome! See you next year! 😄",
        "You're welcome! 🙂 Have a great day!",
        "You're welcome! May the karma gods shine favourably upon you ✨",
    ]

    logger.info(f"Replying to thank you message from {post.author}")
    text = random.choice(options)
    response = post.reply(text)
    logger.info(f"Link: https://www.reddit.com{response.permalink}")


def run(sub):

    logger.info(f"Running r/{sub}")
    subreddit = reddit.subreddit(sub)

    stream = praw.models.util.stream_generator(
        lambda **kwargs: submissions_and_comments(subreddit, **kwargs),
        pause_after=5,
    )

    for post in stream:
        # Get the time now in UTC.
        tnow = datetime.datetime.utcnow()

        # Sleep for 20 minutes ~23:45 to give the DB a chance to be purged.
        if tnow.hour == 23 and tnow.minute > 45:
            logger.info(f"Sleeping for 16 minutes UTC (currently {tnow})")
            time.sleep(60 * 20)

        if post is None:
            continue

        if hasattr(post.author, "is_suspended") and post.author.is_suspended:
            logger.warning("Encountered suspended user, skipping")
            continue

        # Get the date / time posted, and check it was today.
        time_posted = datetime.datetime.utcfromtimestamp(post.created_utc)
        logger.debug(f"{sub} : {post} @ {time_posted}")
        if not time_posted.date() == tnow.date():
            continue

        # Check the author hasn't been deleted.
        author = post.author
        if not author:
            continue

        post_if_cakeday(post, tnow)
        post_if_response_to_cakeday_wish(post, tnow)


@logger.catch
def run_with_exception_handling(sub):
    try:
        run(sub)
    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except Exception as E:
        logger.warning(
            f"Encountered {E}, sleeping for 60 seconds and retrying"
        )
        time.sleep(60)
        run_with_exception_handling(sub)


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


subs = ["cakeday", "FreeKarma4U"]

for sub in subs:
    fx = partial(run_with_exception_handling, sub=sub)
    run_threaded(fx)

schedule.every().day.at("01:00").do(remove_old_cakedays)

while True:
    schedule.run_pending()
    time.sleep(1)
