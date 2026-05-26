/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.util.Random;

/**
 * Procedural ClickBench-shaped dataset generator.
 *
 * <p>Emits NDJSON in OpenSearch _bulk format (alternating action + source lines). The
 * action lines are bare {@code {"index":{}}} so the destination index is supplied via
 * the request URL.
 *
 * <p>The generator is seeded so the same {@code (docCount, seed)} pair always produces
 * identical bytes — deterministic across runs and machines. Distributions are tuned to
 * exercise correctness edge cases beyond the canonical ClickBench shapes:
 *
 * <ul>
 *   <li><b>Tied-row determinism</b> — high-cardinality fields (UserID, WatchID) get
 *       Zipf-distributed repeats, so {@code GROUP BY UserID ORDER BY count(*) DESC} has
 *       a clear winner instead of a sea of count=1 ties.
 *   <li><b>ClickBench-specific anchors</b> — Q20 needs UserID=435090932899640449 to
 *       appear at least once; Q37-Q43 need {@code CounterID=62} rows in July 2013;
 *       Q41 needs {@code RefererHash=3594120000172545465}; Q42 needs
 *       {@code URLHash=2868770270353813622}. The generator plants these explicitly.
 *   <li><b>Null/empty handling</b> — keyword fields fall back to "" with a fixed
 *       ratio so {@code WHERE field <> ''} filters meaningfully.
 *   <li><b>Numeric range</b> — longs cap at {@link Long#MAX_VALUE} (no overflow into
 *       BigInteger which OpenSearch rejects); shorts/ints stay well within their type
 *       range so no value-range mapping errors at ingest.
 *   <li><b>Date coverage</b> — EventTime/EventDate span 2013-06-15 through 2013-08-15
 *       so query Q37's {@code [2013-07-01, 2013-07-31]} window matches a meaningful
 *       slice (~50% of docs) rather than 0 or 100%.
 * </ul>
 *
 * <p>Why this isn't backed by a static fixture: with a static {@code bulk.json} we'd
 * have to commit megabytes of generated text every time we tune the distribution, and
 * we couldn't parameterize tests on dataset size without checking in multiple copies.
 * Procedural generation lets the test ask for "10k docs, seed=42" or "1k docs, seed=7"
 * without touching the repo.
 *
 * @opensearch.internal
 */
final class ClickBenchDataGenerator {

    /** Q20 anchor — must appear at least once so the equality filter returns rows. */
    private static final long ANCHOR_USER_ID = 435090932899640449L;
    /** Q41 anchor — referrer hash for the in-list filter test. */
    private static final long ANCHOR_REFERER_HASH = 3594120000172545465L;
    /** Q42 anchor — URL hash for the equality filter test. */
    private static final long ANCHOR_URL_HASH = 2868770270353813622L;

    /** July 2013 = the window Q37-Q43 filter on. Span 2 months around it. */
    private static final long EVENT_RANGE_START_MS = 1371254400000L; // 2013-06-15 00:00:00 UTC
    private static final long EVENT_RANGE_END_MS = 1376524800000L;   // 2013-08-15 00:00:00 UTC

    private static final String[] SEARCH_PHRASES = {
        "", "", "", "", "",  // bias toward empty so WHERE SearchPhrase <> '' filters meaningfully
        "cheap flights", "openai chatgpt", "weather today", "rust programming",
        "docker tutorial", "best restaurants", "python tutorial", "clickhouse benchmark",
        "machine learning", "opensearch"
    };
    private static final String[] PHONE_MODELS = {
        "", "", "",  // bias toward empty
        "Pixel", "iPhone", "OnePlus", "Samsung Galaxy"
    };
    private static final String[] URLS = {
        "https://reddit.com/r/tech", "https://reddit.com/r/programming",
        "https://news.google.com/world", "https://news.google.com/tech",
        "https://www.google.com/search?q=test", "https://www.google.com/maps",
        "https://example.com/page1", "https://example.com/page2",
        "https://stackoverflow.com/questions/123", "https://github.com/opensearch-project"
    };
    private static final String[] TITLES = {
        "Product Page", "Home Page", "About Us", "Contact",
        "Google Search Results", "Google Maps", "Article Title",
        "Documentation - Google APIs", "Tutorial Page"
    };
    private static final String[] REFERERS = {
        "", "",  // bias toward empty
        "https://reddit.com/r/programming", "https://news.google.com/",
        "https://www.bing.com/", "https://www.example.com/landing",
        "https://stackoverflow.com/"
    };
    private static final String[] BROWSER_LANGS = { "en", "de", "fr", "ja", "zh", "es", "pt" };
    private static final String[] BROWSER_COUNTRIES = { "US", "DE", "FR", "JP", "CN", "BR", "GB", "IN" };
    private static final String[] HIT_COLORS = { "A", "B", "C", "D", "E" };

    private ClickBenchDataGenerator() {}

    /**
     * Generates {@code docCount} documents into NDJSON form for an OpenSearch _bulk
     * request. Output is deterministic for a given {@code (docCount, seed)} pair.
     */
    static String generate(int docCount, long seed) {
        Random rng = new Random(seed);
        StringBuilder out = new StringBuilder(docCount * 1500);
        for (int i = 0; i < docCount; i++) {
            out.append("{\"index\":{}}\n");
            appendDoc(out, rng, i, docCount);
            out.append('\n');
        }
        return out.toString();
    }

    private static void appendDoc(StringBuilder out, Random rng, int docIndex, int docCount) {
        // Anchor docs: guarantee at least one row matches Q20/Q41/Q42 filters.
        boolean anchorUser = docIndex == 0;
        boolean anchorReferer = docIndex == 1;
        boolean anchorUrl = docIndex == 2;
        // ~50% of docs land in July 2013 (Q37-Q43 window) by clamping the generated
        // timestamp to that window for the first half of the dataset.
        boolean inJulyWindow = docIndex < docCount / 2;
        // ~5% of docs get CounterID=62 (Q37-Q43 filter); rest get a random counter id.
        boolean isAnalyticsCounter = docIndex % 20 == 0;

        long eventTimeMs;
        if (inJulyWindow) {
            // 2013-07-01 to 2013-07-31
            eventTimeMs = 1372636800000L + (long) (rng.nextDouble() * (1375228800000L - 1372636800000L));
        } else {
            eventTimeMs = EVENT_RANGE_START_MS + (long) (rng.nextDouble() * (EVENT_RANGE_END_MS - EVENT_RANGE_START_MS));
        }
        long eventDateMs = eventTimeMs - (eventTimeMs % 86400000L);
        long localEventTimeMs = eventTimeMs - rng.nextInt(3600000);
        long clientEventTimeMs = eventTimeMs - rng.nextInt(60000);

        long userId = anchorUser ? ANCHOR_USER_ID : zipfLong(rng, 200);
        // WatchID needs to repeat for Q32/Q33 (group by WatchID, ClientIP) to produce
        // count > 1 buckets — without this every group is a singleton and the
        // top-10 is fully tied.
        long watchId = zipfLong(rng, 100);
        long fUniqId = positiveLong(rng);
        long refererHash = anchorReferer ? ANCHOR_REFERER_HASH : positiveLong(rng);
        long urlHash = anchorUrl ? ANCHOR_URL_HASH : positiveLong(rng);
        long paramPrice = rng.nextInt(10000);

        int counterId = isAnalyticsCounter ? 62 : rng.nextInt(1000);
        // ClientIP needs to repeat for Q31/Q32/Q33/Q36 (group by ClientIP) to produce
        // count > 1 buckets — same reason as WatchID above. Use a smaller pool so we
        // get clear winners.
        int clientIp = (rng.nextInt(5) > 0) ? 100000000 + rng.nextInt(50) : rng.nextInt(Integer.MAX_VALUE);
        int regionId = rng.nextInt(300);
        short advEngineId = (short) (rng.nextInt(20));  // 0-19, biased to 0
        if (rng.nextInt(3) != 0) advEngineId = 0;  // ~67% AdvEngineID=0

        out.append('{');
        appendField(out, "WatchID", watchId, true);
        appendField(out, "JavaEnable", (short) rng.nextInt(2), false);
        appendField(out, "Title", pick(rng, TITLES), false);
        appendField(out, "GoodEvent", (short) 1, false);
        appendField(out, "EventTime", eventTimeMs, false);
        appendField(out, "EventDate", eventDateMs, false);
        appendField(out, "CounterID", counterId, false);
        appendField(out, "ClientIP", clientIp, false);
        appendField(out, "RegionID", regionId, false);
        appendField(out, "UserID", userId, false);
        appendField(out, "CounterClass", (short) rng.nextInt(2), false);
        appendField(out, "OS", (short) rng.nextInt(20), false);
        appendField(out, "UserAgent", (short) rng.nextInt(10), false);
        appendField(out, "URL", pick(rng, URLS), false);
        appendField(out, "Referer", pick(rng, REFERERS), false);
        appendField(out, "IsRefresh", (short) (rng.nextInt(4) == 0 ? 1 : 0), false);
        appendField(out, "RefererCategoryID", (short) rng.nextInt(50), false);
        appendField(out, "RefererRegionID", rng.nextInt(300), false);
        appendField(out, "URLCategoryID", (short) rng.nextInt(50), false);
        appendField(out, "URLRegionID", rng.nextInt(300), false);
        appendField(out, "ResolutionWidth", (short) RESOLUTIONS[rng.nextInt(RESOLUTIONS.length)], false);
        appendField(out, "ResolutionHeight", (short) HEIGHTS[rng.nextInt(HEIGHTS.length)], false);
        appendField(out, "ResolutionDepth", (short) 32, false);
        appendField(out, "FlashMajor", (short) rng.nextInt(20), false);
        appendField(out, "FlashMinor", (short) rng.nextInt(10), false);
        appendField(out, "FlashMinor2", (short) rng.nextInt(5), false);
        appendField(out, "NetMajor", (short) rng.nextInt(5), false);
        appendField(out, "NetMinor", (short) rng.nextInt(10), false);
        appendField(out, "UserAgentMajor", (short) rng.nextInt(100), false);
        appendField(out, "UserAgentMinor", String.valueOf(rng.nextInt(100)), false);
        appendField(out, "CookieEnable", (short) rng.nextInt(2), false);
        appendField(out, "JavascriptEnable", (short) 1, false);
        appendField(out, "IsMobile", (short) rng.nextInt(2), false);
        appendField(out, "MobilePhone", (short) rng.nextInt(5), false);
        appendField(out, "MobilePhoneModel", pick(rng, PHONE_MODELS), false);
        appendField(out, "Params", "", false);
        appendField(out, "IPNetworkID", rng.nextInt(10000), false);
        appendField(out, "TraficSourceID", (short) (rng.nextInt(10) - 1), false);  // -1 to 8
        appendField(out, "SearchEngineID", (short) rng.nextInt(5), false);
        appendField(out, "SearchPhrase", pick(rng, SEARCH_PHRASES), false);
        appendField(out, "AdvEngineID", advEngineId, false);
        appendField(out, "IsArtifical", (short) 0, false);
        appendField(out, "WindowClientWidth", (short) RESOLUTIONS[rng.nextInt(RESOLUTIONS.length)], false);
        appendField(out, "WindowClientHeight", (short) HEIGHTS[rng.nextInt(HEIGHTS.length)], false);
        appendField(out, "ClientTimeZone", (short) (rng.nextInt(25) - 12), false);
        appendField(out, "ClientEventTime", clientEventTimeMs, false);
        appendField(out, "SilverlightVersion1", (short) 0, false);
        appendField(out, "SilverlightVersion2", (short) 0, false);
        appendField(out, "SilverlightVersion3", 0, false);
        appendField(out, "SilverlightVersion4", (short) 0, false);
        appendField(out, "PageCharset", "UTF-8", false);
        appendField(out, "CodeVersion", rng.nextInt(200), false);
        appendField(out, "IsLink", (short) rng.nextInt(2), false);
        appendField(out, "IsDownload", (short) (rng.nextInt(10) == 0 ? 1 : 0), false);
        appendField(out, "IsNotBounce", (short) rng.nextInt(2), false);
        appendField(out, "FUniqID", fUniqId, false);
        appendField(out, "OriginalURL", "", false);
        appendField(out, "HID", rng.nextInt(Integer.MAX_VALUE), false);
        appendField(out, "IsOldCounter", (short) 0, false);
        appendField(out, "IsEvent", (short) 0, false);
        appendField(out, "IsParameter", (short) 0, false);
        appendField(out, "DontCountHits", (short) (rng.nextInt(4) == 0 ? 1 : 0), false);
        appendField(out, "WithHash", (short) 0, false);
        appendField(out, "HitColor", pick(rng, HIT_COLORS), false);
        appendField(out, "LocalEventTime", localEventTimeMs, false);
        appendField(out, "Age", (short) rng.nextInt(80), false);
        appendField(out, "Sex", (short) rng.nextInt(2), false);
        appendField(out, "Income", (short) rng.nextInt(10), false);
        appendField(out, "Interests", (short) rng.nextInt(10000), false);
        appendField(out, "Robotness", (short) 0, false);
        appendField(out, "RemoteIP", rng.nextInt(Integer.MAX_VALUE), false);
        appendField(out, "WindowName", 0, false);
        appendField(out, "OpenerName", 0, false);
        appendField(out, "HistoryLength", (short) rng.nextInt(20), false);
        appendField(out, "BrowserLanguage", pick(rng, BROWSER_LANGS), false);
        appendField(out, "BrowserCountry", pick(rng, BROWSER_COUNTRIES), false);
        appendField(out, "SocialSourceNetworkID", (short) 0, false);
        appendField(out, "SocialSourcePage", "", false);
        appendField(out, "ParamPrice", paramPrice, false);
        appendField(out, "ParamOrderID", "", false);
        appendField(out, "ParamCurrency", "", false);
        appendField(out, "ParamCurrencyID", (short) 0, false);
        appendField(out, "OpenstatServiceName", "", false);
        appendField(out, "OpenstatCampaignID", "", false);
        appendField(out, "OpenstatAdID", "", false);
        appendField(out, "OpenstatSourceID", "", false);
        appendField(out, "UTMSource", "", false);
        appendField(out, "UTMMedium", "", false);
        appendField(out, "UTMCampaign", "", false);
        appendField(out, "UTMContent", "", false);
        appendField(out, "UTMTerm", "", false);
        appendField(out, "FromTag", "", false);
        appendField(out, "HasGCLID", (short) rng.nextInt(2), false);
        appendField(out, "RefererHash", refererHash, false);
        appendField(out, "URLHash", urlHash, false);
        appendField(out, "CLID", rng.nextInt(1000), false);
        appendField(out, "HTTPError", (short) 0, false);
        appendField(out, "SendTiming", rng.nextInt(2000), false);
        appendField(out, "DNSTiming", rng.nextInt(500), false);
        appendField(out, "ConnectTiming", rng.nextInt(500), false);
        appendField(out, "ResponseStartTiming", rng.nextInt(2000), false);
        appendField(out, "ResponseEndTiming", rng.nextInt(5000), false);
        appendField(out, "FetchTiming", rng.nextInt(2000), false);
        out.append('}');
    }

    /** Common screen widths (low-cardinality dimension for GROUP BY). */
    private static final int[] RESOLUTIONS = { 1024, 1280, 1366, 1440, 1920, 2560 };
    /** Common screen heights. */
    private static final int[] HEIGHTS = { 600, 720, 768, 800, 900, 1080, 1200, 1218 };

    /** Strictly positive long, never overflows. */
    private static long positiveLong(Random rng) {
        return rng.nextLong() & Long.MAX_VALUE;
    }

    /**
     * Generates a long that's biased to repeat — about {@code repeatBucket} distinct
     * values fill ~80% of draws, the rest are unique. Lets group-by-userid produce
     * meaningful counts (a clear winner) without all-1 distributions.
     */
    private static long zipfLong(Random rng, int repeatBucket) {
        if (rng.nextInt(5) > 0) {
            // 80%: pick from a small bucket of "popular" userids
            return 1000000000000000000L + rng.nextInt(repeatBucket);
        }
        // 20%: unique random userid
        return positiveLong(rng);
    }

    private static String pick(Random rng, String[] options) {
        return options[rng.nextInt(options.length)];
    }

    private static void appendField(StringBuilder out, String name, long value, boolean first) {
        if (!first) out.append(',');
        out.append('"').append(name).append("\":").append(value);
    }

    private static void appendField(StringBuilder out, String name, int value, boolean first) {
        if (!first) out.append(',');
        out.append('"').append(name).append("\":").append(value);
    }

    private static void appendField(StringBuilder out, String name, short value, boolean first) {
        if (!first) out.append(',');
        out.append('"').append(name).append("\":").append(value);
    }

    private static void appendField(StringBuilder out, String name, String value, boolean first) {
        if (!first) out.append(',');
        out.append('"').append(name).append("\":\"").append(escapeJson(value)).append('"');
    }

    private static String escapeJson(String s) {
        if (s.indexOf('"') < 0 && s.indexOf('\\') < 0 && s.indexOf('\n') < 0) return s;
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c == '\n') sb.append("\\n");
            else sb.append(c);
        }
        return sb.toString();
    }
}
