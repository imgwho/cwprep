classDiagram
direction BT
class node1 {
   varchar(100) customer_name  /* 客户姓名 */
   enum('consumer', 'corporate', 'home office') segment  /* 客户类型 */
   varchar(20) customer_id  /* 客户编号 */
}
class node0 {
   varchar(20) order_id  /* 订单编号 */
   date order_date  /* 订单日期 */
   date ship_date  /* 发货日期 */
   enum('standard class', 'second class', 'first class', 'same day') ship_mode  /* 配送方式 */
   varchar(20) customer_id  /* 客户编号 */
   int region_id  /* 区域编号 */
   varchar(100) city  /* 城市 */
   varchar(100) state  /* 州/省 */
   varchar(20) postal_code  /* 邮编 */
   varchar(20) product_id  /* 产品编号 */
   decimal(12,4) sales  /* 销售额 */
   int quantity  /* 数量 */
   decimal(5,2) discount  /* 折扣率 */
   decimal(12,4) profit  /* 利润 */
   int row_id
}
class node2 {
   varchar(255) product_name  /* 产品名称 */
   enum('furniture', 'office supplies', 'technology') category  /* 大类 */
   varchar(50) sub_category  /* 子类 */
   varchar(20) product_id  /* 产品编号 */
}
class node4 {
   varchar(50) region_name  /* 区域名称 */
   varchar(100) manager_name  /* 区域经理 */
   int region_id
}
class node3 {
   varchar(20) order_id  /* 订单编号 */
   enum('yes', 'no') returned  /* 是否退货 */
   int return_id
}

node0  -->  node1 : customer_id
node0  -->  node2 : product_id
node0  -->  node4 : region_id
