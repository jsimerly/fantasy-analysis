"""Sleeper API URL helpers (api/_utils.py): build_route, add_filters."""

import sleeper_ingestion.api._utils as mod
build_route = mod.build_route
add_filters = mod.add_filters


class TestBuildRoute:
    def test_joins_base_and_paths(self):
        assert build_route("https://api.sleeper.app/v1", "league", "123") == \
            "https://api.sleeper.app/v1/league/123"

    def test_strips_trailing_slash_on_base(self):
        assert build_route("https://api.sleeper.app/v1/", "league") == \
            "https://api.sleeper.app/v1/league"

    def test_coerces_int_path_segments_to_str(self):
        assert build_route("https://x.test", "league", 123, "matchups", 4) == \
            "https://x.test/league/123/matchups/4"

    def test_strips_internal_slashes_on_segments(self):
        # a segment passed with surrounding slashes must not double up
        assert build_route("https://x.test", "/league/", "/123/") == \
            "https://x.test/league/123"


class TestAddFilters:
    def test_first_filter_uses_question_mark(self):
        assert add_filters("https://x.test/v", ("week", 3)) == \
            "https://x.test/v?week=3"

    def test_subsequent_filters_use_ampersand(self):
        assert add_filters("https://x.test/v", ("a", 1), ("b", 2)) == \
            "https://x.test/v?a=1&b=2"

    def test_existing_query_string_starts_with_ampersand(self):
        assert add_filters("https://x.test/v?x=0", ("week", 3)) == \
            "https://x.test/v?x=0&week=3"
