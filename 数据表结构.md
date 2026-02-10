数据库版本：MySQL 8.0.28  
时间相关的函数建议用，unix\_timestamp('2025-07-01 00:00:00')

# **常用tableau高频计算**

//日期  
DATEADD('hour', 8, DATEADD('second', \[Time\], \#1970-01-01\#))

//时间  
DATEADD('second',INT(\[check\_time\]),\#1970-01-01 08:00:00\#)

# **订单表（e\_product\_order\_list）**

订单营业额计算：trash \= 0，is\_self\_buy=0，然后total\_price/1000000+raise\_price/1000000-discount/1000000  
订单表的status：  
已发货 3 4 5 6 7 8 14 15 16 18 19  
已签收 4,6  
已审核 2,3,4,5,6,7,8,10,11,12,14,15,16,17,18,19  
when a.status \='0' then '待处理'  
when a.status \='1' then '待审核'  
when a.status \='2' then '待发货'  
when a.status \='3' then '快递中'  
when a.status \='4' then '已签收'  
when a.status \='5' then '拒签件'  
when a.status \='6' then '已回款'  
when a.status \='7' then '已退件'  
when a.status \='8' then '问题件'  
when a.status \='9' then '驳回'  
when a.status \='10' then '财务审核'  
when a.status \='11' then '发货异常'  
when a.status \='12' then '赠品审核'  
when a.status \='13' then '取消单'  
when a.status \='14' then '理赔'  
when a.status \='15' then '破损'  
when a.status \='16' then '京东退中通'  
when a.status \='17' then '预约发货'

不出单的公司:所有的新媒体、策划部、客户服务部、研发部、人工智能、接待中心 、所有的招商部  
以下公司排除：  
贵州犇富分公司  
南宁分公司  
四川创泽团队  
总部定制酒招商  
总部行政  
总部招商  
贵阳旅游项目  
旅游公司  
深圳新媒体  
招商部  
总部私域团队  
总部外呼业务部

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| :---- | :---- | :---- | :---- | :---- |
| id | int AUTO\_INCREMENT | \- | 非空 | 索引（主键） |
| oid | bigint unsigned | 0 | 非空 | 部门 |
| company\_id | int unsigned | 0 | 非空 | 分公司 |
| acc | bigint unsigned | 0 | 非空 | 员工/工号 |
| original\_order | varchar(512) | \- | 可空 | 原始单号 |
| platform\_sn | varchar(512) | '' | 非空 | 平台单号 |
| order\_sn | varchar(512) | \- | 可空 | 订单号 |
| name | varchar(128) | '' | 非空 | 客户名称 |
| contact | varchar(512) | \- | 可空 | 详细地址 |
| gender | int unsigned | 0 | 非空 | 性别 |
| province | bigint unsigned | 0 | 非空 | 省 |
| city | bigint unsigned | 0 | 非空 | 市 |
| area | bigint unsigned | 0 | 非空 | 区 |
| street | bigint unsigned | 0 | 非空 | 街道 |
| freight | bigint unsigned | 0 | 非空 | 运费 |
| supply\_price | bigint | 0 | 非空 | 供货价 |
| raise\_price | bigint unsigned | 0 | 非空 | 套餐加价金额 |
| total\_price | bigint unsigned | 0 | 非空 | 套餐总金额 |
| discount | bigint unsigned | 0 | 非空 | 优惠金额 |
| trash | int unsigned | 0 | 非空 | 是否删除 |
| ~~createtime~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~创建时间~~ |
| ~~updatetime~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~更新时间~~ |
| acc\_number | int unsigned | 0 | 非空 | 职员编码 |
| user\_id | int | 0 | 非空 | 会员ID |
| wechat\_add | tinyint unsigned | 0 | 非空 | 线上加微 |
| number | int unsigned | 0 | 非空 | 数量 |
| is\_self\_buy | tinyint(1) | 0 | 非空 | 是否自购 |
| ~~enable~~ | ~~enum('N', 'Y')~~ | ~~'Y'~~ | ~~非空~~ | ~~是否有效~~ |
| ~~gid~~ | ~~varchar(32)~~ | ~~''~~ | ~~可空~~ | ~~无明确注释~~ |
| ~~bid~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~商户~~ |
| ~~master~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~师傅~~ |
| ~~share~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~分享部门~~ |
| ~~source~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~订单来源~~ |
| ~~splits~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~订单拆分~~ |
| ~~age~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~年龄段~~ |
| ~~delivery~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~仓库~~ |
| ~~repetition~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~是否复购单~~ |
| ~~remark~~ | ~~varchar(512)~~ | ~~\-~~ | ~~可空~~ | ~~客户备注~~ |
| ~~combo~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~套餐名称~~ |
| ~~combo\_price~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~套餐单价~~ |
| ~~call\_key~~ | ~~varchar(128)~~ | ~~''~~ | ~~非空~~ | ~~通话记录ID~~ |
| ~~table~~ | ~~varchar(32)~~ | ~~''~~ | ~~非空~~ | ~~通话记录表~~ |
| ~~supply\_amount~~ | ~~bigint~~ | ~~0~~ | ~~非空~~ | ~~供货金额~~ |
| ~~raise\_freight~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~运费加价金额~~ |
| ~~money~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~预付金额~~ |
| ~~inherit\_money~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~预付继承金额~~ |
| ~~collection~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~代收贷款~~ |
| ~~deposit~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~保证金~~ |
| ~~status~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~状态~~ |
| ~~audit\_status~~ | ~~tinyint unsigned~~ | ~~0~~ | ~~非空~~ | ~~审核状态~~ |
| ~~jd\_status~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~京东状态~~ |
| ~~zot\_status~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~中通状态~~ |
| ~~logistics~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~物流公司~~ |
| ~~ware\_group~~ | ~~tinyint unsigned~~ | ~~1~~ | ~~非空~~ | ~~配送点~~ |
| ~~settlement~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~是否结算~~ |
| ~~settlement\_month~~ | ~~varchar(32)~~ | ~~''~~ | ~~非空~~ | ~~结算月~~ |
| ~~eclp\_no~~ | ~~varchar(32)~~ | ~~''~~ | ~~非空~~ | ~~京东单号~~ |
| ~~check\_time~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~审核日期~~ |
| ~~stock\_time~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~库存结余日期~~ |
| ~~tracking\_number~~ | ~~varchar(800)~~ | ~~''~~ | ~~非空~~ | ~~快递单号~~ |
| ~~refused\_reason~~ | ~~varchar(64)~~ | ~~''~~ | ~~非空~~ | ~~驳回原因~~ |
| ~~delivery\_time~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~发货时间~~ |
| ~~collection\_time~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~收款时间~~ |
| ~~record\_times~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~录单次数~~ |
| ~~return\_time~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~回款/退货时间~~ |
| ~~sign\_time~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~签收日期~~ |
| ~~tracking\_remark~~ | ~~varchar(512)~~ | ~~\-~~ | ~~可空~~ | ~~物流备注~~ |
| ~~customer\_id~~ | ~~bigint unsigned~~ | ~~0~~ | ~~非空~~ | ~~分配来源~~ |
| ~~repurchase~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~复购更新~~ |
| ~~common\_type~~ | ~~tinyint(1)~~ | ~~0~~ | ~~非空~~ | ~~0 为之前的套餐，1 现在的套餐~~ |
| ~~review~~ | ~~tinyint unsigned~~ | ~~0~~ | ~~非空~~ | ~~预收复核~~ |
| ~~is\_settle~~ | ~~tinyint(1)~~ | ~~1~~ | ~~非空~~ | ~~是否结算~~ |
| ~~delayed\_deliver~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~延时发货~~ |
| ~~isadd\_wechat~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~是否添加微信~~ |
| ~~earnest\_money~~ | ~~bigint~~ | ~~0~~ | ~~非空~~ | ~~定金~~ |
| ~~ewid~~ | ~~varchar(20)~~ | ~~''~~ | ~~可空~~ | ~~员工微信ID~~ |
| ~~cwid~~ | ~~varchar(20)~~ | ~~''~~ | ~~可空~~ | ~~员工微信ID~~ |

# **员工表（bi\_e\_admin）**

员工表的id=订单表的acc作为关联，人数统计的时候用员工表number

1个员工可能有多个id，但只有1个number工号

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| ----- | ----- | ----- | ----- | ----- |
| id | BIGINT | 无 | 否 | 员工主键 ID |
| username | VARCHAR(50) | 无 | 否 | 员工姓名 |
| number | INT | 无 | 否 | 员工工号 |
| gender | VARCHAR(2) | 无 | 是 | 性别（男 / 女 / 未知） |
| age | INT | 无 | 是 | 年龄 |
| education | VARCHAR(32) | 无 | 是 | 学历名称 |
| marital | VARCHAR(10) | 无 | 是 | 婚姻状况（已婚已育 / 已婚未育 / 未婚 / 未知） |
| role\_group | VARCHAR(10) | 无 | 是 | 角色组（主管 / 员工 等） |
| job\_status | VARCHAR(12) | 无 | 是 | 在职状态（在职 / 离职…） |
| entry\_time | DATE | 无 | 是 | 入职时间 |
| leave\_time | DATE | 无 | 是 | 离职时间 |
| work\_days | INT | 无 | 是 | 在职天数 |
| self\_type | VARCHAR(6) | 无 | 是 | 是否自营（自营 / 代理 / 未知） |
| filialeid | BIGINT | 无 | 是 | 分公司 ID |
| filialename | VARCHAR(50) | 无 | 是 | 分公司名称 |
| departmentid | BIGINT | 无 | 是 | 部门 ID |
| departmentname | VARCHAR(50) | 无 | 是 | 部门名称 |
| groupid | BIGINT | 无 | 是 | 班组 ID |
| groupname | VARCHAR(50) | 无 | 是 | 班组名称 |
| admin\_status | VARCHAR(30) | 无 | 是 | e\_admin.status 原值 |

# **订单产品表（e\_order\_product）**

关联条件：通过order\_id跟订单表的id进行关联

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| :---- | :---- | :---- | :---- | :---- |
| order\_id | bigint unsigned | \- | 非空 | 订单ID（主键） |
| combo\_type | tinyint unsigned | '0' | 非空 | 套餐类型， 0-普通, 1-引流, 2-赠送, 3-高端引流, 4-郎酒, 5-直播 |
| goods\_id | int unsigned | '0' | 非空 | 商品ID |
| goods\_name | varchar(512) | '' | 非空 | 商品名称 |
| combo\_id | int unsigned | '0' | 非空 | 套餐ID |
| combo\_name | varchar(512) | '' | 非空 | 套餐名称 |
| number | int unsigned | '0' | 非空 | 数量 |
| bottle | int unsigned | '0' | 非空 | 瓶数 |
| price | bigint unsigned | '0' | 非空 | 单价 |
| total\_price | bigint unsigned | '0' | 非空 | 商品总价 |
| stock | int unsigned | '0' | 非空 | 购买库存 |
| discount | bigint unsigned | '0' | 非空 | 折扣 |
| unit | int unsigned | '0' | 非空 | 单位 |
| product\_no | varchar(128) | '' | 非空 | 产品编号 |
| image | varchar(256) | '' | 非空 | 产品图片 |
| label | varchar(128) | '' | 非空 | 产品标签 |
| level | tinyint unsigned | '0' | 非空 | 产品定位 |
| area\_limit | varchar(255) | \- | 可空 | 发货一区限制组织 |
| area | varchar(255) | \- | 可空 | 发货区 |
| warehouse | mediumtext | \- | 可空 | 配送点配置 |
| ~~createtime~~ | ~~int unsigned~~ | ~~'0'~~ | ~~非空~~ | ~~创建时间~~ |
| ~~updatetime~~ | ~~int unsigned~~ | ~~'0'~~ | ~~非空~~ | ~~更新时间~~ |
| activityName | varchar(256) | \- | 可空 | 活动名称 |
| activityId | int unsigned | '0' | 非空 | 活动ID |
| version | tinyint unsigned | '0' | 非空 | 版本 |
| settlement | text | \- | 非空 | 提成结算方案 |

# **地理区域表（e\_area\_code）**

关联条件：通过province跟订单表的provide进行关联

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| :---- | :---- | :---- | :---- | :---- |
| code | bigint unsigned AUTO\_INCREMENT | \- | 非空 | 区划代码 |
| name | varchar(128) | '' | 非空 | 名称 |
| level | tinyint(1) | \- | 非空 | 级别1-5,省市县镇村 |
| pcode | bigint | 0 | 可空 | 父级区划代码 |
| express | varchar(256) | '' | 非空 | 快递 |
| price | varchar(256) | '' | 非空 | 加价 |
| collection | varchar(256) | '' | 非空 | 不到代收 |
| restrict | varchar(256) | '' | 非空 | 限重5公斤 |
| unreachable | varchar(256) | '' | 非空 | 长期不发货 |
| field1 | varchar(255) | '' | 非空 | 预留字段1 |
| field2 | varchar(255) | '' | 非空 | 预留字段2 |
| field3 | varchar(255) | '' | 非空 | 预留字段3 |
| field4 | varchar(255) | '' | 非空 | 预留字段4 |
| field5 | varchar(255) | '' | 非空 | 预留字段5 |
| enable | enum ('N', 'Y') | 'Y' | 非空 | 是否有效 |
| remark | varchar(255) | '' | 非空 | 备注 |
| province | varchar(55) | '' | 非空 | 省 |
| city | varchar(55) | '' | 非空 | 市 |
| district | varchar(55) | '' | 非空 | 区 |
| town | varchar(55) | '' | 非空 | 镇 |
| village | varchar(55) | '' | 非空 | 村镇 |

# **微信聊天记录表7月（e\_vdata\_message\_2025\*\*）**

每个月有一个单独的表

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| :---- | :---- | :---- | :---- | :---- |
| id | bigint | \- | 非空 | 索引（自增主键） |
| UserName | varchar(128) | '' | 非空 | 坐席/销售微信ID |
| Talker | varchar(128) | '' | 非空 | 好友/客户微信ID |
| MsgId | varchar(32) | '0' | 可空 | 消息ID/房间ID |
| Time | int unsigned | 0 | 非空 | 消息时间 |
| Type | tinyint unsigned | 0 | 非空 | 消息类型（具体值说明：1=文字消息；3=图片消息；34=语音消息；37=好友确认消息；42=卡片；43=视频消息；47=表情包；48=定位；49=xml消息；50=拒绝；51=最新消息ID标记红点；10000=友情提示；2000=转账） |
| SubType | tinyint unsigned | 0 | 非空 | 消息子类型（具体值说明：2000=P2P转账；2001=群收款；5000010=同意视频通话；5000020=同意语音通话；5010001=红包） |
| Direct | tinyint unsigned |  | 非空 | 聊天来源/渠道： 1=工作台;2=OA;3=AI;100=人工;10000=PHP |
| IsSender | tinyint unsigned | 0 | 非空 | 是否销售发送（1=是；0=否） |
| Status | tinyint unsigned | 0 | 非空 | 是否有效通话 |
| StartTime | int unsigned | 0 | 非空 | 通话开始时间 |
| EndTime | int unsigned | 0 | 非空 | 通话结束时间 |
| Cid | bigint unsigned | 0 | 非空 | 分公司标识 |
| Oid | bigint unsigned | 0 | 非空 | 部门标识 |
| UserId | bigint unsigned | 0 | 非空 | 员工标识 |
| SplitFieTime | int unsigned | 0 | 非空 | 消息产生时间 |
| Xml | varchar(256) | '' | 非空 | 腾讯返回的xml信息 |
| ~~Createtime~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~记录创建时间~~ |
| ~~Updatetime~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~记录更新时间~~ |
| Coid | int unsigned | 0 | 非空 | 分公司部门标识 |
| Cbid | int unsigned | 0 | 非空 | 分公司班组标识 |
| only | varchar(32) | '' | 可空 | 由Talker+Time+Content+UserName生成的md5值 |

# **微信客户记录表（e\_vdata\_contact）**

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| :---- | :---- | :---- | :---- | :---- |
| id | int AUTO\_INCREMENT | \- | 非空 | 索引（主键） |
| WeChatId | varchar(128) | '' | 非空 | 坐席/销售微信ID |
| UserName | varchar(128) | '' | 非空 | 好友/客户微信ID |
| Status | int | \-1 | 可空 | 状态 |
| ~~Createtime~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~创建时间~~ |
| ~~Updatetime~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~更新时间~~ |
| Company | int unsigned | 0 | 非空 | 分公司 |
| ~~Department~~ | ~~int unsigned~~ | ~~0~~ | ~~非空~~ | ~~部门 弃用~~ |
| UserId | int unsigned | 0 | 非空 | 员工/销售ID |
| ClientType | tinyint unsigned | 0 | 非空 | 客户分类（0-无；1-活跃客户；2-低活跃客户；3-沉默客户；4-客户删除；5-客户拉黑） |
| Client | tinyint(1) | 0 | 非空 | 复购分类 |
| CurrTime | int unsigned | 0 | 非空 | 最近联系时间 |
| CurrBack | int unsigned | 0 | 非空 | 最近回复时间 |
| Repurchase | tinyint unsigned | 0 | 非空 | 复购分类 |
| WorkMate | tinyint unsigned | 0 | 非空 | 是否同事 |
| Lastmoney | float unsigned | 0 | 非空 | 上月营业额 |
| Thismoney | float unsigned | 0 | 非空 | 本月营业额 |
| CurrDeal | int unsigned | 0 | 非空 | 最近成交时间 |
| Class | int unsigned | 0 | 非空 | 班组 |
| Depart | int unsigned | 0 | 非空 | 部门 |
| CallName | varchar(20) | '' | 非空 | 称呼 |
| Birthday | varchar(20) | '' | 非空 | 生日 |
| Sex | tinyint unsigned | 0 | 非空 | 性别 |
| Birthtype | tinyint unsigned | 0 | 非空 | 是否阳历 |
| Birthyear | varchar(20) | '' | 非空 | 出生年份 |
| Source | tinyint unsigned | 0 | 非空 | 来源 |
| Country | varchar(10) | '' | 非空 | 国家 |
| Province | varchar(20) | '' | 非空 | 省 |
| City | varchar(20) | '' | 非空 | 市 |
| FreshTime | int unsigned | 0 | 非空 | api刷新时间 |
| DeleteTime | int unsigned | 0 | 非空 | 删除拉黑时间 |
| Enable | enum ('N', 'Y'，) | 'Y' | 非空 | 是否有效 |
| AddTime | varchar(10) | '' | 非空 | 添加时间 |
| NickNameSpell | varchar(128) | '' | 非空 | 昵称首字母 |
| RemarkSpell | varchar(256) | '' | 非空 | 备注首字母 |
| LabelStr | varchar(512) | '' | 非空 | 标签 |

# **微信客户号码关联表（e\_vdata\_wechat\_phones）**

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| :---- | :---- | :---- | :---- | :---- |
| Id | int AUTO\_INCREMENT | \- | 非空 | 索引（主键） |
| WeChatId | varchar(20) | '微信ID' | 非空 | 坐席/销售微信ID |
| UserName | varchar(20) | '好友ID' | 非空 | 好友/客户微信ID |
| UserId | varchar(1026) | '' | 非空 | 坐席 |
| Company | int unsigned | 0 | 非空 | 分公司 |
| Depart | int unsigned | 0 | 非空 | 部门 |
| Class | int unsigned | 0 | 非空 | 班组 |
| lastmoney2 | decimal(18, 2\) unsigned | 0 | 非空 | 上月营业额 |
| thismoney2 | decimal(18, 2\) unsigned | 0 | 非空 | 本月营业额 |
| lastmoney | decimal(18, 2\) unsigned | 0 | 非空 | 上月营业额 |
| thismoney | decimal(18, 2\) unsigned | 0 | 非空 | 本月营业额 |
| user | int unsigned | 0 | 非空 | 会员ID |

# **营销活动（e\_marketing\_eventlist）**

| 字段名 | 类型 | 默认值 | 是否为空 | 字段含义 |
| :---- | :---- | :---- | :---- | :---- |
| id | int AUTO\_INCREMENT | 自增 | NOT NULL | 索引 |
| oid | bigint unsigned | 0 | NOT NULL | 组织 |
| acc | bigint unsigned | 0 | NOT NULL | 反馈人 |
| name | varchar(255) | '' | NOT NULL | 活动名称 |
| starttime | int unsigned | 0 | NOT NULL | 开始日期 |
| endtime | int unsigned | 0 | NOT NULL | 结束日期 |
| status | int unsigned | 0 | NOT NULL | 状态（0 \- 筹备中 1 \- 进行中 2 \- 已结束） |
| content | varchar(512) | '' | NOT NULL | 描述 |
| createtime | int unsigned | 0 | NOT NULL | 创建时间 |
| updatetime | int unsigned | 0 | NOT NULL | 更新时间 |

