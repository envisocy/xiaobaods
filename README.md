# Xiaobaods 后台程序说明文档
> :: 宝儿电商 数据组 ::

### fc
- 01:2017-04-10
	- ver_03 03-20 对文件结构进行了调整，建立主函数程序
	- ver_04 04-02 尝试优化排序算法
	- ver_05 04-06 放弃集合排序算法，未来独立开发算法程序
	- ver_06 04-10 优化代码，修补掉包可能的读取数据库的bug
    - ver_07 04-11 添加w的debug=9的CSV导出到桌面功能(继承于a_02)
    - ver_08 04-12 debug运行时间反馈
	- ver_10 04-14 修补重排BUG，整体代码优化，加入ws EDT排序
    - ver_11 04-27 修补服务器BUG
    - ver_12 04-28 使用索引技术优化MySQL读取性能，并对SQL语句逻辑进行优化，针对conf文件进行优化调整
    - ver_13 05-11 添加xiaobaods_c.py函数
    - ver_14 05-15 添加'storegroup.txt'的店铺组信息读取，并添加a/c的storechoice和storegroupchoice参数,对结果进行筛选。
    - ver_15 05-15 storechoice排斥storegroupchoice优先，且针对天猫店铺后缀一定程度容错
    - ver_16 05-25 针对线上txt编码问题重新写入txt读取模块，引入包codecs
    - ver_17 05-27 添加针对Panel的xiaobaods_m文档
    - ver_18 06-01 更新m，并优化fillna参数:添加bd(自动填充空值为行后方值，只能填充一次，并删除所有处理后的含空行)
    - ver_19 06-03 重做_ws，舍弃之前的LR算法，改为Log算法
    
### a
三大类搜索数据的历史数据（排名）组合表
> from:"bc_attribute_granularity_sales","bc_attribute_granularity_visitor"
##### version:
- 01:2017-03-17 
	- ver_01 基础内容更新
	- ver_05 停用，并计划未来更新删除
	- ver_10版本删除
- a:2017-04-10(原a_02)
	- ver_03 改为def内嵌import
	- ver_03 修补Visitor表的BUG
	- ver_04 添加‘查看详情’、‘同款货源’变量
	- ver_05 添加path参数（与debug=9配合使用，默认桌面）
	- ver_06 添加debug=9的CSV导出到桌面功能
	- ver_08 加入含debug参数后返回运行时间反馈
    - ver_11 统一命名为a
	- ver_12 大量修改SQL查询逻辑
    - ver_14 storechoice > storegroup choice参数,对结果进行筛选
    - ver_15 后缀一定程度容错（对“旗舰店”、“女装旗舰店”、“专营店”等）
    - ver_18 更新fillna

### w
搜索热词的历史数据（排名）组合表
> from:"bc_searchwords_hotwords","bc_searchwords_risewords"
##### version:
- 01:2017-03-15
	- ver_02 根据代号a的开发，进行基础框架开发
	- ver_03 实现a_02绝大部分可实现功能
	- ver_05 停用
    - ver_10 彻底删除
- w:2017-04-11(原w_03)
	- ver_03 代号测试算法排序实现
	- ver_05 添加fillna参数
	- ver_07 添加debug=9的CSV导出到桌面功能(继承于a_02)
	- ver_10 统一为最新编译，修复重排BUG，加入对应ws EDT排序整合
    - ver_11 统一命名为w
    - ver_12 将w剥离为不同功能的w和ws，分开维护，优化SQL语句逻辑。
    - ver_18 更新fillna
- ws:2017-04-28(Alg)
    - ver_12 两次调用debug=2返回值和一次variable=排序进行针对"排序"的排序，通过head参数调整输出长度
    - ver_19 重做_ws，舍弃之前的LR算法，改为Log算法

### c
属性搜索数据历史数据（排名）组合表
> from:"bc_category_granularity"
##### version:
- c:2017-05-11
    - ver_13 根据a添加classification和attributes参数，删除table参数，并对数据采集没有“查看详情”变量进行调整
    - ver_14 storechoice > storegroup choice参数,对结果进行筛选
    - ver_15 后缀一定程度容错
    - ver_18 更新fillna

### m
Panel中的类目同比/环比数据对比
> from:"bc_industry_market"
##### version:
- m:2017-06-02
    - ver_17 新参数display和vs同步输出df1和df0两张数据表单
    - ver_18 更新产生df表针对df1和df0的组合方式