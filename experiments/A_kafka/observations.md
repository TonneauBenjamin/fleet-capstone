# Part A - notifier partition assignment (on the laptop)

Raw outputs: `notifier_rebalance.log` (final code), `notifier_rebalance_run1_old_code.log`,
`notifier_rebalance_crash_before_fix.log`.

## Final run (`notifier_rebalance.log`, 12:44 - 12:46 UTC)
1. Start: notifier1 = [1], notifier2 = [0, 2], lag 0.
2. `docker kill notifier2` at 12:44:36. At 12:45:19 (43 s later, session timeout 45 s, a SIGKILL
   sends no LeaveGroup) notifier1 gets [2, 0] -> [0, 1, 2]; it never gave up partition 1.
3. `docker start notifier2` at 12:45:36. Cooperative rebalance: notifier1 is asked to give back
   only partition 0... but 3 of its synchronous commits fail with `ILLEGAL_GENERATION` (the group
   generation changed during the rebalance), its assignment is then considered lost
   (`revoked [1, 2]`), and the final split is notifier1 = [0, 2], notifier2 = [1].
   Both notifiers keep running and the lag goes back to 0.
4. The alerts whose commit failed are read again by their new owner: at-least-once, a few
   duplicated notifications, nothing lost.

## First run with the first version of the code (`notifier_rebalance_run1_old_code.log`, 11:58 - 12:00)
Same scenario, lower alert rate: no commit happened during the rebalance window and we got the
"textbook" cooperative-sticky result: after the restart, only partition 0 moved back to notifier2,
notifier1 kept [1, 2].

## The bug found on the way (`notifier_rebalance_crash_before_fix.log`, 11:55)
The very first try crashed notifier1: `commit()` raised `KafkaException ILLEGAL_GENERATION` during
the rebalance and the exception was not caught. Fix in `consumers/alerts_notifier.py`: catch the
exception around the commit, log it and continue (the partition belongs to someone else now, who
will re-read the alert). We had not seen it in our first tests because there were almost no alerts.
