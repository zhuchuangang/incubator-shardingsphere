grammar MySQLStatement;

import MySQLKeyword, Keyword, MySQLDQL, MySQLBase, Symbol, MySQLDML, DQLBase, MySQLCreateIndex
       , MySQLDropIndex, MySQLCreateTable, MySQLAlterTable, MySQLDropTable, MySQLTruncateTable
       , MySQLTCLStatement, MySQLDCLStatement
       ;

execute
    : (select
    | insert
    | update
    | delete
    | replace
    | createIndex
    | dropIndex
    | createTable
    | alterTable
    | dropTable
    | truncateTable
    | beginTransaction
    | setAutoCommit
    | commit
    | rollback
    | setTransaction
    | savepoint
    | grant
    | grantProxy
    | grantRole
    | revoke
    | revokeAll
    | revokeProxy
    | revokeRole
    | createUser
    | alterUser
    | alterCurrentUser
    | alterUserRole
    | dropUser
    | renameUser
    | createRole
    | dropRole
    | setPassword
    | setDefaultRole
    | setRole
    )SEMI_? 
    ;