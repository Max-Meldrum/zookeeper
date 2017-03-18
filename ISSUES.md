# Moving to QuorumFlexible

5/5 test in ReadOnlyModeTest fails: Cause startQuorum() function in QuorumUtil.
3 tests fail in QuorumTest, here cause is also startQuorum().

TODO: Actually verify that the QuorumFlexible works. It seems to work when running benchmarks. But need to check and understand why the unit tests fails, they are probably designed for a majority quorum.
