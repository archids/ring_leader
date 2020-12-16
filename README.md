# ring_leader
Ring Leader Election Algorithm - Python

Leader Election Algorithm for processes connected in a ring, logical or random.
The processes are able to select a Leader at the startup if there is none or work with a default Leader if provided.
Elected Leader renews Lease at regular intervals.  The other processes keep track of these lease renewals and if they haven't received one within the set timeout, can initiate new Leader Election, simultaneously, thereby adding redundancy and failure tolerance.
Once the Leader Election cycle comes back to the initiator, it will process and select the Leader based on a criteria such as "Value".  It will send out another message to all process with the selected Leader information.  All other process will update their record with the new Leader.  If multiple election is going on, Processes will wait till all complete their cycle and agree on the Leader, which will be same, if elected leader has not failed during this process.  Then the election process goes through the same cycle.
Once Leader is elected, it will begin the Lease renewal process.
All processes tag their ID to the Lease, Voting or Leader notification messages, thereby broadcasting their "liveness".

This is a raw version that works and may need further code refinement.
