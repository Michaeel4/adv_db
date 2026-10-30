# Homework 2 - Solution

Advanced Databases, WS 2026/27.

Blocking.java is the original program. The tasks update overlapping value ranges, so they lock the same rows and wait for each other. That's a deadlock, and Postgres aborts one of the tasks. In the original code that task is simply lost.

RestartBlocking.java: when a task gets aborted, it does a rollback, closes the connection and starts again until it commits.

NoLiveLocks.java: same as RestartBlocking, but every task counts its rollbacks. After 2 rollbacks the task locks the whole table first, so it runs alone and can't be the deadlock victim again. This way no task starves.

To run it you need Postgres, the JDBC driver (pom.xml) and your own ConnectionFactory.java. It's not in the repo.
