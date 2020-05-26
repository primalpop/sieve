# Paper abstract

This paper considers the challenge of a scalable implementation of Fine-Grained Access Control (FGAC) over large corpus of policies in database systems. Current approaches of enforcing fine-grained policies based on rewriting queries do not scale when the number of policies are in the order of thousands. This paper identifies one such use case in the context of emerging smart spaces wherein systems may be required by legislation, such as Europe's GDPR and California's CCPA, to empower users to specify who may have access to their data and for what purposes. We present Sieve, a layered approach of implementing FGAC in existing database systems, that exploits a variety of features (index support, UDFs, hints) to scale to large number of policies. Given a set of policies, Sieve generates guarded expressions that might exploit database indices to filter tuples and given a query exploit context to filter policies that need to be checked. Our experimental results show that Sieve scales to large data sets and to large policy corpus thus supporting real-time access in applications including emerging smart environments.

## Policy Simulator


