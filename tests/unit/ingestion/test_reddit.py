"""Tests for Reddit adapter record mapping."""

from __future__ import annotations

from unittest.mock import MagicMock

from pymander.ingestion.adapters.reddit import (
    _extract_media_urls,
    process_comment,
    process_submission,
)
from pymander.schemas.enums import ContentType, Platform


def _make_submission(**overrides):
    """Create a mock Reddit submission."""
    sub = MagicMock()
    sub.id = "abc123"
    sub.title = "Test post title"
    sub.selftext = "This is the post body text."
    sub.permalink = "/r/test/comments/abc123/test_post_title/"
    sub.created_utc = 1700000000.0
    sub.score = 42
    sub.num_comments = 10
    sub.num_crossposts = 0
    sub.view_count = None
    sub.url = "https://reddit.com/r/test/comments/abc123/"
    sub.subreddit = MagicMock()
    sub.subreddit.__str__ = lambda self: "test"
    sub.upvote_ratio = 0.95
    sub.is_self = True
    sub.link_flair_text = None
    sub.over_18 = False
    sub.spoiler = False
    sub.stickied = False
    sub.domain = "self.test"
    sub.is_gallery = False
    sub.is_video = False
    sub.media = None
    sub.all_awardings = []

    author = MagicMock()
    author.__str__ = lambda self: "testuser"
    author.__bool__ = lambda self: True
    author.created_utc = 1600000000.0
    author.has_verified_email = True
    author.comment_karma = 1000
    author.link_karma = 500
    sub.author = author

    for k, v in overrides.items():
        setattr(sub, k, v)
    return sub


def _make_comment(**overrides):
    """Create a mock Reddit comment."""
    comment = MagicMock()
    comment.id = "xyz789"
    comment.body = "This is a test comment."
    comment.permalink = "/r/test/comments/abc123/test/xyz789/"
    comment.created_utc = 1700001000.0
    comment.score = 5
    comment.parent_id = "t3_abc123"
    comment.depth = 0
    comment.is_submitter = False
    comment.controversiality = 0
    comment.distinguished = None
    comment.stickied = False
    comment.replies = []

    author = MagicMock()
    author.__str__ = lambda self: "commenter"
    author.__bool__ = lambda self: True
    author.has_verified_email = False
    comment.author = author

    for k, v in overrides.items():
        setattr(comment, k, v)
    return comment


class TestProcessSubmission:
    async def test_basic_submission(self):
        sub = _make_submission()
        record = await process_submission(sub)

        assert record.platform == Platform.REDDIT
        assert record.content_type == ContentType.POST
        assert record.platform_content_id == "abc123"
        assert "Test post title" in record.text
        assert "post body text" in record.text
        assert record.title == "Test post title"
        assert record.actor.username == "testuser"
        assert record.engagement.likes == 42
        assert record.engagement.replies == 10
        assert record.id is not None

    async def test_deleted_author(self):
        sub = _make_submission(author=None)
        record = await process_submission(sub)
        assert record.actor.username == "[deleted]"
        assert record.actor.profile_url is None

    async def test_url_contains_permalink(self):
        sub = _make_submission()
        record = await process_submission(sub)
        assert "reddit.com" in record.url
        assert "abc123" in record.url

    async def test_raw_payload_has_subreddit(self):
        sub = _make_submission()
        record = await process_submission(sub)
        assert record.raw_payload is not None
        assert record.raw_payload["subreddit"] == "test"

    async def test_serialization_roundtrip(self):
        sub = _make_submission()
        record = await process_submission(sub)
        from pymander.schemas.content import UnifiedContentRecord

        data = record.model_dump(mode="json")
        restored = UnifiedContentRecord.model_validate(data)
        assert restored.platform == Platform.REDDIT
        assert restored.actor.username == "testuser"


class TestProcessComment:
    async def test_basic_comment(self):
        comment = _make_comment()
        record = await process_comment(comment, "abc123")

        assert record.platform == Platform.REDDIT
        assert record.content_type == ContentType.COMMENT
        assert record.platform_content_id == "xyz789"
        assert record.text == "This is a test comment."
        assert record.root_id == "abc123"
        assert record.parent_id == "t3_abc123"

    async def test_comment_engagement(self):
        comment = _make_comment(score=100)
        record = await process_comment(comment, "abc123")
        assert record.engagement.likes == 100


class TestMediaExtraction:
    def test_direct_image(self):
        sub = _make_submission(url="https://i.imgur.com/test.jpg")
        urls = _extract_media_urls(sub)
        assert "https://i.imgur.com/test.jpg" in urls

    def test_no_media(self):
        sub = _make_submission(url="https://reddit.com/r/test/comments/abc123/")
        urls = _extract_media_urls(sub)
        assert urls == []

    def test_video(self):
        sub = _make_submission(
            is_video=True,
            media={"reddit_video": {"fallback_url": "https://v.redd.it/test/DASH_720.mp4"}},
        )
        urls = _extract_media_urls(sub)
        assert "https://v.redd.it/test/DASH_720.mp4" in urls
