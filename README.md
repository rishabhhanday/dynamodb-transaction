# dynamodb-transaction

With the transaction write API, you can group multiple Put, Update, Delete, and ConditionCheck actions.
You can then submit the actions as a single TransactWriteItems operation that either succeeds or fails as a unit.

The same is true for multiple Get actions, which you can group and submit as a single TransactGetItems operation.

NOTE : DynamoDB performs two underlying reads or writes of every item in the transaction: one to prepare the transaction and one to commit the transaction.
