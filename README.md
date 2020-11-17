# TicketD 

Ticketd is a service that allows access to shared recources via tickets. Services can issue one or more tickets to access a specific resource. Clients
can claim a ticket for a particular resource and do work against the resource while the ticket remains claimed. Once finished, the client releases the ticket, which makes it 
available to the next client..

TickedtD also supports shared locks, so that processes across a network can aquire and release locks.

Access to ticketd is maintained through named entities called sessions. Services issuing tickets and clients both use sessions. Sessions are created with a specified ttl; when
that ttl expires, a session is automatically closed, and any tickets issued against a resource by that session are removed. Any tickets claimed by that session are released. 
Any session that holds a lock releases it upon expiration or close.

Sessions can be kept alive by requesting a refresh fron the ticketd server, which resets the expiration timer. The go client library includes support for background refreshes.

Ticketd is very fast and uses comparatively few resources. While sessions, resources and locks are kept in memory, the server can be set to snapshot its internal state at
intervals. This snapshot is then reloaded upon server restart.

Access is through either the Go client library, or the underying REST api.
