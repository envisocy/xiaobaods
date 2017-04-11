--- a ---
三大类搜索数据的历史数据（排名）组合表
from:"bc_attribute_granularity_sales","bc_attribute_granularity_visitor"
version:
01:2017-03-17 
	- 基础内容更新
02:2017-03-17 
	- 改为def内嵌import
	- 修补Visitor表的BUG
	- 添加‘查看详情’、‘同款货源’变量
	- 添加path参数（与debug=9配合使用，默认桌面）
	- 添加debug=9的CSV导出功能
	- 加入含debug参数后返回运行时间反馈

--- w ---
搜索热词的历史数据（排名）组合表
from:"bc_searchwords_hotwords","bc_searchwords_risewords"
version:
01:2017-03-15
	- 根据代号a的开发，进行基础框架开发
	- 实现a_02绝大部分可实现功能
03:2017-03-15
	- 03代号测试算法排序实现
	- 添加fillna参数

