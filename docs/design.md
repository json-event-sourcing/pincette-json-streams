# Designing Applications

The structure of an event-driven application focusses on the end-user use-cases in a 1-to-1 way. It is about recording everything the user does and supporting this with lists of items, coming from search, browsing or anything else.

When a user is making changes or creating an item, their work should be captured with one aggregate instance. So, there is a direct mapping between the screen and the aggregate. When something is committed on the screen, this always results in a command that is sent to the aggregate instance. The latter will emit the change as an event. If the command had validation errors, the command, with error annotations, is emitted instead. Both kinds of events should be relayed back to the user asynchronously.

It is not necessary to let the user wait for the result of the submission. As soon as the confirmation of delivery is received, the screen can be closed. If there are validation errors, the part of the UI that deals with the incoming events can reopen the screen with the original data and the error annotations. If the screen was still open, it can be annotated there. If the screen was closed and should not be reopened automatically, a light notification can make the user aware of the problem. This could be something like a badge with the number of errors, which, when clicked, opens a list of error reports. Clicking on one can then reopen the original screen. A refinement could be to reopen the screen automatically anyway if the user has not yet opened a new context. 

The point is that a power user should never be waiting for something. They can work through a list of tasks very quickly, knowing that in the vast majority of cases there will be no validation errors. It would be a pity to let them wait always because of exceptional cases.

Users normally work on different items. So, there would always be some kind of list to start with. This could be the result of a search, a recent items list, a task list, a browse, etc. Such lists should come from simple queries that project everything away that is not needed to present the list.

When a list is a view over several underlying aggregates, it is tempting to do a query that mimics a join. With MongoDB that would be the `$lookup` aggregation pipeline stage. However, it is not made for that and even an "efficient" join would amount to a relative complex query that is repeated very often.

It is better to pre-compose the elements of a view. Say there are three underlying aggregates that contribute to a view. The view itself should have no more that five or six fields. Wider views are not focussed enough. We can incrementally materialise the view through an aggregate. For each contributing aggregate there would be a consumer that turns the events into commands for the view aggregate.

The events of the latter can also be relayed to the user. The item in the list on the screen can simply be updated. There is no need to refresh the list with another query. It is good to give the user a clue about an upcoming update by changing some visual aspect of the item in the list. It could be greyed out and disabled, for example.

[![Graph](design.svg)](design.svg)

A materialised view should keep the lineage of the fields that were contributed by the underlying aggregates. This makes it possible to open detail screens for those aggregates. These screens are not related to the view as such. They can appear in several situations.

So, in general the sequence goes as follows:

1. A list of items is shown.
1. The user opens an aggregate detail for some item in the list.
1. The user makes changes to the aggregate instance and submits them.
1. The aggregate instance emits an event, which is also relayed back to the user.
1. A reactor consumes the event and translates it into a command for the list aggregate instance that corresponds to the selected item.
1. The list aggregate instance emits an event, which is also relayed back to the user.
1. The item in the list is updated.