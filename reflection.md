**Just some thoughts as I go**
- Don't like Java, but I hope it's gonna be worth it :smiley: 
- Started this project on my own just to fill the gaps in my knowledge optimizers` inner workings and to finally play with Calcite as a nice bonus
- Calcite starter documentation is a crap IMO. I already had a pleasure of trying to understand how it works years ago when working at heavy.ai, but to no success. Unfortunately, it didn't get any better with years.
- As I understand, CMU students can get some help from TA and from each other. Myself, unfortunately, can only rely on search engines and github forks (as a last resort, hope not to go there)
- The most useful resource for building a query optimizer with Calcite so far turns out to be this blog https://www.querifylabs.com/blog as they show how to run planner with basic set of rules. They also occasionally go deeper into specific topics.
- The second most useful resource is Calcite unit tests. Can grab some building blocks from there
- For the beginning I've decided to go with JdbcSchema to read from duckdb. But it doesn't work correctly cuz it cannot understand DuckDB`s BASE TABLE type. Will have to write my own schema or add some hacks on top of JDBC schema
- I see that I can call Volcano planer in at least two ways: as a Calcite Program and as findBestExp. Not sure which should I use, just opted for the latter due to its simplicity. Probably Program is more flexible, as I understand
- After quite a bit of trying I managed to get a set of rules to execute my Jdbc physical nodes. Had some troubles with enumerable scan, still not sure what helped to get it going.
- Decided to commit my first iteration in this status:
- - 9 queries pass
- - 9 queries timeout
- - 8 queries fail, mostly due to `variable $cor0 is not found`
- I've decided to do statistics "the right way" - that is implementing my own schema and in-memory table type.
- The result is almost no queries are passing due to many unsupported conversions and type casts (still no idea why it tries to convert date to int in q7).
- Another weirdness - when I add unique columns stats Calcite gives me bogus SQL which is a) plain wrong; b) ambiguous. Had to forfeit this idea for now, though I expected it to be helpful in many rewrites and cost estimates.
- Kinda regret trying to write my own scannable table type. I could have made things much easier just by extending JdbcSchema with stats.
- At this point I still feel quite blind: docs still suck and everything looks like a bunch of random changes to see what works, without real understanding why.

**Conclusions**

TODO