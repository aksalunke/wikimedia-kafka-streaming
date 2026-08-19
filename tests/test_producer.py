from producer.producer import derive_language, is_canary_event


class TestDeriveLanguage:
    def test_simple_language_wiki(self):
        assert derive_language("enwiki") == "en"

    def test_sister_project_wikisource(self):
        # Real edge case from the live stream: Chinese Wikisource
        assert derive_language("zhwikisource") == "zh"

    def test_sister_project_wiktionary(self):
        assert derive_language("dewiktionary") == "de"

    def test_non_language_wiki_wikidata(self):
        # Real edge case from the live stream
        assert derive_language("wikidatawiki") == "other"

    def test_non_language_wiki_commons(self):
        assert derive_language("commonswiki") == "other"

    def test_unrecognized_wiki_falls_back_to_other(self):
        assert derive_language("notarealwikiformat") == "other"

    def test_bare_wiki_suffix_with_nothing_before_it(self):
        # Stripping "wiki" from exactly "wiki" leaves "", the `or "other"` catch
        assert derive_language("wiki") == "other"

    def test_empty_string_input(self):
        assert derive_language("") == "other"


class TestIsCanaryEvent:
    def test_canary_event_detected(self):
        assert is_canary_event({"meta": {"domain": "canary"}}) is True

    def test_real_event_not_flagged(self):
        assert is_canary_event({"meta": {"domain": "en.wikipedia.org"}}) is False

    def test_missing_meta_does_not_crash(self):
        assert is_canary_event({}) is False