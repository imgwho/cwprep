classDiagram
direction BT
class node1 {
   varchar(100) customer_name  /* Customer Name */
   enum('Consumer', 'Corporate', 'Home Office') segment  /* Segment */
   varchar(20) customer_id  /* Customer ID */
}
class node0 {
   varchar(20) order_id  /* Order ID */
   date order_date  /* Order Date */
   date ship_date  /* Ship Date */
   enum('standard class', 'second class', 'first class', 'same day') ship_mode  /* Ship Mode */
   varchar(20) customer_id  /* Customer ID */
   int region_id  /* Region ID */
   varchar(100) city  /* City */
   varchar(100) state  /* State */
   varchar(20) postal_code  /* Postal Code */
   varchar(20) product_id  /* Product ID */
   decimal(12,4) sales  /* Sales */
   int quantity  /* Quantity */
   decimal(5,2) discount  /* Discount */
   decimal(12,4) profit  /* Profit */
   int row_id
}
class node2 {
   varchar(255) product_name  /* Product Name */
   enum('furniture', 'office supplies', 'technology') category  /* Category */
   varchar(50) sub_category  /* Sub-Category */
   varchar(20) product_id  /* Product ID */
}
class node4 {
   varchar(50) region_name  /* Region Name */
   varchar(100) manager_name  /* Regional Manager */
   int region_id
}
class node3 {
   varchar(20) order_id  /* Order ID */
   enum('yes', 'no') returned  /* Returned */
   int return_id
}

node0  -->  node1 : customer_id
node0  -->  node2 : product_id
node0  -->  node4 : region_id
