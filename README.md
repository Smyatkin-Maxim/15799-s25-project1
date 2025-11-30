
# 15-799 Spring 2025 Project 1

  

Please see the project [writeup](https://15799.courses.cs.cmu.edu/spring2025/project1.html) for details, and the upstream repo for how to run this project.

## The results

It's hard to evaluate any exact numbers as Gradescope is very unreliable in its measurements, but I the final code usually gets somewhere between 92 and 107. Usually it's around 102.
The final submission looked like this:
| solution  | score | median speedup | mean speedup | num >= x1.5 speedup | num timeout
|--|--|--|--|--|--|
| my | 107.6 | 1.357 | 534.593 | 21 | 0 |
|--|--|--|--|--|--|
| refsol | 90.8 | 1.025 | 1.046 | 3 | 3 |

  

## How it works
The solution runs two different optimizers (one with bushy joins and one without) and evaluates those two against each other. Also, both of them are two-stage optimizers. I first run a HEP phase with a few obvious optimizations and de-correlation and then I run a cost-based Volcano planner. The rulesets for both optimizers are hand-picked and somewhat tuned for the benchmark.
There are also some rules, heuristics, statistics and metadata providers written by myself.

The most important things that worked:
- Provide valid row counts. Nothing gonna work well without it
- Penalize joins where the inner side is bigger than the outer. I`m really surprised Calcite doesn't do this by default but this is probably the simplest and the most useful thing to do (after we provide some basic stats ofc)
- De-corellation is another must have feature
- And of course some basic rules like filter pushdowns and Top N sort optimization. Other rules didn't seem to much too much difference.

Some things that help, but not that much:
- NDV statistics together with unique keys give better cardinality and cost estimates
- Bushy joins help sometimes. More often they don't. But it's important at least to consider those, hence I evaluate both bushy and non-bushy optimizers against each other
- Equal predicate selection estimates based on NDV give way much better cardinality estimates and plans for some queries. Default Calcite selectivity functions are way too naive
- LIKE, NOT LIKE selection - same as above, but applied less often
- Common factors pull-up - a believe only one query needed this optimization but it was worth it

And there were some things that didn't help at all:
- Relying purely on cost based optimizer to come up with the best join plan didn't work cuz Calcite is a) way too naive in its estimates, b) ignores CPU costs of different join implementations and c) doesn't care about on-disk joins and memory estimates. So unless provided with lots of cost functions OR good heuristics - it fails terribly.
- A simple idea to reorder predicates based on their selictivity and costs proved to be not that simple in Calcite. Again - neither selectivity nor cost functions are good there. So to make it work one need to implement both.
- Using Calcite default ruleset doesn't work. The only way to design the optimizer here it to start from a small number of optimizations and work step by step to improve on it.