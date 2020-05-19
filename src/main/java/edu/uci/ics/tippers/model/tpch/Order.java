package edu.uci.ics.tippers.model.tpch;

/**
 * +-----------------+---------------+------+-----+---------+-------+
 * | Field           | Type          | Null | Key | Default | Extra |
 * +-----------------+---------------+------+-----+---------+-------+
 * | O_ORDERKEY      | int           | NO   |     | NULL    |       |
 * | O_CUSTKEY       | int           | NO   | MUL | NULL    |       |
 * | O_ORDERSTATUS   | char(1)       | NO   |     | NULL    |       |
 * | O_TOTALPRICE    | decimal(15,2) | NO   |     | NULL    |       |
 * | O_ORDERDATE     | date          | NO   |     | NULL    |       |
 * | O_ORDERPRIORITY | char(15)      | NO   |     | NULL    |       |
 * | O_CLERK         | char(15)      | NO   |     | NULL    |       |
 * | O_SHIPPRIORITY  | int           | NO   |     | NULL    |       |
 * | O_COMMENT       | varchar(79)   | NO   |     | NULL    |       |
 * +-----------------+---------------+------+-----+---------+-------+
 */

public class Order {

    private int order_key;

    private int cust_key;

    private String order_status;

    private double total_price;

    private String order_priority;

    private String clerk;

    private int ship_priority;

    private String  comment;

}
