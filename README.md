### fc
- 01:2017-04-10
	- ver_03 03-20 对文件结构进行了调整，建立主函数程序
	- ver_04 04-02 尝试优化排序算法
	- ver_05 04-06 放弃集合排序算法，未来独立开发算法程序
	- ver_05 s 04-10 优化代码，修补掉包可能的读取数据库的bug
	- ver_10 04-12 修补重排BUG，整体代码优化，加入ws EDT排序
    - ver_11 04-27 修补服务器BUG

### a
三大类搜索数据的历史数据（排名）组合表
> from:"bc_attribute_granularity_sales","bc_attribute_granularity_visitor"
version:
- 01:2017-03-17 
	- 基础内容更新
	- ver05停用，并计划未来更新删除
	- 10版本删除
- 02:2017-04-10 
	- 改为def内嵌import
	- 修补Visitor表的BUG
	- 添加‘查看详情’、‘同款货源’变量
	- 添加path参数（与debug=9配合使用，默认桌面）
	- 添加debug=9的CSV导出到桌面功能
	- 加入含debug参数后返回运行时间反馈
- 2017-04-12
	- 统一为最新编译，修复重排BUG

### w
搜索热词的历史数据（排名）组合表
> from:"bc_searchwords_hotwords","bc_searchwords_risewords"
version:
- 01:2017-03-15
	- 根据代号a的开发，进行基础框架开发
	- 实现a_02绝大部分可实现功能
	- 该版本于ver04停用并于ver05彻底删除
- 03:2017-04-11
	- 03代号测试算法排序实现
	- 添加fillna参数
	- 添加debug=9的CSV导出到桌面功能(继承于a_02)
- 2017-04-12
	- 统一为最新编译，修复重排BUG，加入对应ws EDT排序整合