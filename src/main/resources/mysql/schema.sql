/* Table creation statements for MySQL */

/* "CREATE TABLE IF NOT EXISTS policy (\n" +
                        "	id           varchar(255) NOT NULL PRIMARY KEY,\n" +
                        "	description  text NOT NULL,\n" +
                        "	action       text NOT NULL,\n" +
                        "	conditions 	 text NOT NULL\n" +
                        "	objects 	 text NOT NULL\n" +
                        "	queriers 	 text NOT NULL\n" +
                        "	purposes 	 text NOT NULL\n" +
                        "	authors 	 text NOT NULL\n" +
                        "	metadata 	 text NOT NULL\n" +
                        ")",
                "CREATE TABLE IF NOT EXISTS policy_condition (\n" +
                        "	name varchar(255) NOT NULL,\n" +
                        "	type   varchar(255) NOT NULL,\n" +
                        "	key   varchar(255) NOT NULL,\n" +
                        "	value   varchar(255) NOT NULL,\n" +
                        "	relop   varchar(255) NOT NULL,\n" +
                        "	policy  varchar(255) NOT NULL,\n" +
                        "	FOREIGN KEY (policy) REFERENCES policy(id) ON DELETE CASCADE\n" +
                        ")"

                        */