# Mini-ERP: the whale customer

This is a small demo of a cloud business system (an "ERP") used in the exercise
*Refinement of the Whale*. You do not need to read or understand any code to use it.

## The business situation

You run a cloud ERP for small businesses. Every customer (called a *tenant*) has 5 to 20
users who enter orders and look at reports.

- **300 small customers** pay about €99 a month each.
- All of them share **one system and one database**. This keeps costs low.
- A distributor with **1,200 users** wants to join. They would pay **€6,000 a month**
  on a 12-month contract. We call this big customer **the whale**.
- They require **99.9% availability** (a promise that the system works almost all the time)
  and **separation of their data** from everyone else's.

The catch: the system was never built for a customer this big. In a test it had to handle
about 8 times today's peak load, and the database grew by 35%. Worst of all, the whale's
**month-end reports** slowed the test system down for everybody else.

Sales needs a go / no-go decision within a week. The question for the exercise:
**can we take on the whale without hurting our 300 small customers, and what would it cost?**

## What the application shows

The application has two pages.

**Customer** (the main page). This is what one customer sees: their latest orders, a form
for a new order, and a button that creates the monthly report. The label *System speed* in
the top bar shows how long the last action took (green is fast, red is slow).

**Operations.** A control panel for setting up the situation:

- **Remove whale / Add whale** takes the big customer out of the system or puts them back.
- **Simulate month-end** makes the whale's finance team run their monthly reports all at
  once, as they would on the last day of the month.

### Try it yourself

1. Open the **Customer** page and click around as a small customer. Everything is fast.
2. Open **Operations** and click **Simulate month-end**.
3. Go back to the **Customer** page and try again. The system is now slow, and after a few
   seconds the page stops working. This is what the small customers would experience.
4. On **Operations**, click **Remove whale**. Now the small customer's page works again,
   even though the simulation is still running.
5. Click **Add whale** (it takes about 15 seconds) and **Stop simulation** to restore everything.

This is the problem you are asked to solve.

## How to run it

The easiest way is in your web browser. You do not need to install anything.

1. Log in to GitHub and open this repository.
2. Choose your own branch from the branch list at the top left of the repository page.
3. Click the green **Code** button, open the **Codespaces** tab and choose
   **Create codespace on** your branch.
4. Wait a few minutes. The first start builds the application and loads the data.
   When it is ready, a browser tab with the application opens on its own.
5. If it did not open, find the **Ports** tab at the bottom of the screen and click the
   globe icon next to port **8000**.

Notes:

- A Codespace uses your free monthly hours. When you finish, stop it
  (github.com/codespaces, then **Stop codespace**). It also stops by itself after 30
  minutes without activity.
- Pages: the main page is the address of port 8000, and **Operations** is the same address
  followed by `/ops`.

### Running on your own computer (optional)

If you have Docker Desktop installed and running, open a terminal in the project folder and run:

```bash
docker compose up -d --build
```

```bash
docker compose run --rm api python -m app.seed
```

Then open <http://localhost:8000> in your browser. To switch it off:

```bash
docker compose down
```

## Spike results: what this version changes

This version adds two changes that keep the whale's month-end reports from slowing down
the 300 small customers. Both are switched on in `docker-compose.yml`.

**1. Report queue** (`app/guardrail.py`). One customer can run at most 2 heavy reports at
the same time and start at most 20 reports per second. Extra reports wait in line instead
of failing. A waiting report does not take one of the server's workers, so small customers
always find a free one. Normal actions (order list, new order) are never put in the queue.
A report gets the answer "429, retry later" only if it has waited more than 30 seconds.

**2. Report cache** (`app/main.py`). At month-end the finance team asks for the same report
again and again. The report is now calculated once per 60 seconds, and everyone else gets
the saved result. If many requests arrive at once, only one of them calculates. The cache
is cleared when the whale is removed or added on the Operations page.

### Results

Each test runs `loadtest/loadtest.py` for 30 seconds without the whale and 30 seconds with
the whale, and compares how fast small customers are served (p95 response time). "x1.0"
means the whale has no effect on them. Small customers had 0 errors in every run.

| Situation | Before | After |
|---|---|---|
| Shared database, month-end rush (40 reports at once, like "Simulate month-end") | x174.6, p95 1,257 ms | x1.1, p95 8.4 ms |
| Shared database, standard load test (8 report workers) | x6.3, p95 47.6 ms | x1.0, p95 7.7 ms |
| Separate whale database, month-end rush (40 reports at once) | x21.7, p95 166.9 ms | x1.3, p95 8.4 ms |
| Separate whale database, steady demand of 20 reports/s | x7.8, p95 54.1 ms | x1.1, p95 8.2 ms |

Each change on its own, separate whale database, 20 reports/s:

| Setting | Small customers p95 |
|---|---|
| Nothing | x7.8 |
| Report queue only | x2.3 |
| Report cache only | x1.2 |
| Both | x1.1 |

The whale pays for this: in a month-end rush its reports wait in line for up to 2 seconds.
For a monthly report that is acceptable. A separate database for the whale did not help on
its own (x21.7 in a rush), because every report also uses a worker of the shared
application. All raw numbers are in `results/`.

The tests ran on one machine with 2 CPUs. Before go-live they should be repeated on
staging with the real database setup.

### Try the difference yourself

1. Start the application as described above and do the "Try it yourself" steps. With the
   changes on, the Customer page stays fast while the month-end simulation runs.
2. To see the old behaviour, set `REPORT_CONCURRENCY`, `REPORT_RATE` and
   `REPORT_CACHE_SECONDS` to `"0"` in `docker-compose.yml` and run
   `docker compose up -d --build` again.
3. The Operations page data is also available at `/admin/metrics`. It shows the current
   settings and `report_rejections`, the number of reports that waited too long.

### Running the load test and the automatic checks

```bash
python loadtest/loadtest.py --label my-run --whale-workers 40     # month-end rush
python loadtest/loadtest.py --label my-run --whale-rps 20         # steady demand
```

`tests/test_spike.py` has 13 automatic checks: whale data stays in its own database, the
queue never runs more than 2 reports, extra reports wait instead of failing, normal actions
are never queued, a cached report is identical to a fresh one, the cache expires and is
cleared when the whale is removed. They need the whale in its own database (`db_whale` in
`docker-compose.yml`). The file header explains the setup. Run them with
`pip install pytest` and `pytest -v tests/`.
