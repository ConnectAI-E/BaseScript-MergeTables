import { bitable, UIBuilder, IOpenSegmentType } from "@lark-base-open/js-sdk";

/*
Text = 1,
Number = 2,
SingleSelect = 3,
MultiSelect = 4,
DateTime = 5,
Checkbox = 7,
User = 11,
Phone = 13,
Url = 15,
Attachment = 17,
SingleLink = 18,
Lookup = 19,
Formula = 20,
DuplexLink = 21,
Location = 22,
GroupChat = 23,
//*/

export default async function main(uiBuilder: UIBuilder) {

  const fieldType_List_All = [1, 2, 3, 4, 5, 7, 11, 13, 15, 17, 18, 19, 20, 21, 22, 23];
  let options_list: any = [];
  const tables_meta = await bitable.base.getTableMetaList();
  tables_meta.forEach((item: any) => {
    options_list.push({ label: item.name, value: item.id })
  });

  uiBuilder.form((form: any) => ({
    formItems: [
      form.select('table_Source_List', { label: '源数据表', options: options_list, multiple: true, defaultValue: '' }),
      form.select('select_view', {
        label: '源数据表视图名称（不输入或不存在视图则获取全量数据）',
        options: [{ label: '表格', value: '表格' }],
        defaultValue: '',
        tags: true,
      }),
      form.tableSelect('table_Target', { label: '目标数据表' }),
      form.fieldSelect('field_Target', {
        label: '同步字段',
        sourceTable: 'table_Target',
        multiple: true,
        filter: ({ type }: { type: any }) => (fieldType_List_All.indexOf(type) >= 0 && type !== 19 && type !== 20),
        optionFilterProp: "children",
        showSearch: true,
        filterOption: (input: string, option: any) => `${(option?.label ?? '')}`.toLowerCase().includes(input.toLowerCase()),
      }),
      form.checkboxGroup('checkbox_clean_table', {
        label: '',
        options: ['清空目标数据表'],
        defaultValue: []
      }),

    ],
    buttons: ['合并数据'],

  }), async ({ values }: { values: any }) => {
    let { table_Source_List, select_view, table_Target, field_Target, checkbox_clean_table } = values;
    // console.log(values);

    // 分组函数
    function grouping(array: any, subGroupLength: any) {
      let index = 0;
      let newArray = [];
      while (index < array.length) {
        newArray.push(array.slice(index, index += subGroupLength));
      }
      return newArray;
    }

    // 判断以下字段是否进行了选择，如果未选择则提示并返回
    if (!table_Source_List) { alert("请选择源数据表"); return; }
    if (!field_Target) { alert("选择同步字段"); return; }

    if (checkbox_clean_table.length > 0) {
      const recordIdList = await table_Target.getRecordIdList();
      var msg = "\n    请确认是否要清空目标数据表中的 " + String(recordIdList.length) + " 条数据？\n";
      if (confirm(msg) == true) {
        uiBuilder.showLoading('正在清除目标表数据，请稍等...');
        const new_recordIdList = grouping(recordIdList, 5000);
        for (let i = 0; i < new_recordIdList.length; i++) {
          await table_Target.deleteRecords(new_recordIdList[i]);
          // 延迟代码
          if (i < new_recordIdList.length - 1) {
            await new Promise((resolve) => {
              setTimeout(() => {
                resolve("延时结束");
              }, 3000);
            })
          }
        }
      } else { return; }
    }

    uiBuilder.showLoading('开始准备合并数据，请稍等...');

    // 获取数据表的名称
    const table_target = table_Target;

    // 根据选择的字段信息生成包含字段id和name的数组
    let field_name_list: any = [];
    const metaList_target: any = await table_target.getFieldMetaList();
    metaList_target.forEach((target_item: any) => {
      field_Target.forEach((field_item: any) => {
        if (target_item.id == field_item.id) {
          field_name_list.push({ field_id: target_item.id, name: target_item.name, type: target_item.type });
        }
      })
    })

    // console.log(field_name_list);

    // console.log(table_Source_List);
    let merge_field_name_list: any = [];
    let records_update_list: any = [];
    let record_update_list: any = {};
    let fields_update_list: any = { 'fields': {} };
    let select_property: any = {}

    // 循环处理多个源表的数据
    for (const table_Source of table_Source_List) {

      //根据前面的field_name_list重新生成包含name,type和源和目标数据表field_id的数组
      const table_source = await bitable.base.getTableById(table_Source as string);
      const table_source_name = await table_source.getName();
      const metaList_source: any = await table_source.getFieldMetaList();
      merge_field_name_list = [];
      // console.log(metaList_source);
      metaList_source.forEach((source_item: any) => {
        field_name_list.forEach((target_item: any) => {
          if (source_item.name === target_item.name) {
            merge_field_name_list.push({ name: target_item.name, type: target_item.type, field_id: { source_id: source_item.id, target_id: target_item.field_id } })
          }
        })
      })
      // console.log(1, merge_field_name_list);

      let hasMore: boolean = true;
      let pageSize: number = 5000;
      let pageToken: string = "";
      let viewId: string = "";
      let count: number = 0;
      const merge_field_name_list_len = merge_field_name_list.length;
      while (hasMore) {
        // console.log(1, dataindex);

        if (typeof select_view !== 'undefined') {
          if (select_view.length > 0 && select_view[0] !== '') {
            const viewMetaList = await table_source.getViewMetaList();
            viewMetaList.forEach((item: any) => {
              if (item.name === select_view[0]) {
                viewId = item.id;
              }
            })
          }
        }

        const source_recordValueList = await table_source.getRecords({ pageSize: pageSize, pageToken: pageToken, viewId: viewId });
        // console.log(2, source_recordValueList);
        pageToken = source_recordValueList.pageToken || '';
        hasMore = source_recordValueList.hasMore;
        const get_records = source_recordValueList.records;
        const recordid_list_sourcre_len = source_recordValueList.total;
        // console.log(get_records);

        // 循环处理字段数组
        for (var i = 0; i < get_records.length; i++) {
          const record_items: any = get_records[i];
          // console.log(record_items);
          for (var j = 0; j < merge_field_name_list_len; j++) {
            let record_value: any = '';
            const record_item: any = record_items.fields[merge_field_name_list[j].field_id.source_id];
            const merge_field_target_id = merge_field_name_list[j].field_id.target_id;
            // console.log(record_item);

            switch (merge_field_name_list[j].type) {
              case 1: //Text
                record_value = record_item ? record_item[0].text : '';
                record_value = [{ type: IOpenSegmentType.Text, text: record_value }]
                break;
              case 3: //SingleSelect
                record_value = record_item?.text || '';
                // 获取目标表单选字段的选项信息
                let get_ss_options: any = "";
                if (typeof select_property[merge_field_target_id] === 'undefined') {
                  const target_ss_record_item = await table_target.getFieldMetaById(merge_field_target_id);
                  select_property[merge_field_target_id] = target_ss_record_item.property;
                  get_ss_options = target_ss_record_item?.property?.options || '';
                } else {
                  get_ss_options = select_property[merge_field_target_id].options;
                }
                for (var k = 0; k < get_ss_options.length; k++) {
                  if (record_value == get_ss_options[k].name) {
                    record_value = get_ss_options[k];
                    break;
                  }
                }
                break;
              case 4: //MultiSelect
                record_value = record_item || '';
                // 获取目标表多选字段的选项信息
                let get_ms_options: any = "";
                if (typeof select_property[merge_field_target_id] === 'undefined') {
                  const target_ms_record_item = await table_target.getFieldMetaById(merge_field_target_id);
                  select_property[merge_field_target_id] = target_ms_record_item.property;
                  get_ms_options = target_ms_record_item?.property?.options || '';
                } else {
                  get_ms_options = select_property[merge_field_target_id].options;
                }
                let get_ms_options_value: any = [];
                for (var l = 0; l < record_value.length; l++) {
                  for (var k = 0; k < get_ms_options.length; k++) {
                    if (record_value[l].text == get_ms_options[k].name) {
                      get_ms_options_value.push(get_ms_options[k]);
                    }
                  }
                }
                record_value = get_ms_options_value;
                break;
              case 7: //Checkbox
                record_value = record_item ? true : false;
                break;
              case 2: //Number
              case 5: //DateTime
              case 11: //User
              case 13: //Phone
              case 15: //Url
              case 17: //Attachment
              case 18: //SingleLink
              case 21: //DuplexLink
              case 22: //Location
              case 23: //GroupChat
                record_value = record_item;
                break;
              default:
                record_value = null;
                break;
            }
            record_update_list[merge_field_target_id] = record_value;
          }
          uiBuilder.showLoading(`正在处理【` + table_source_name + `】表的第 ` + String(count + 1) + ` / ` + recordid_list_sourcre_len + ` 条记录`);
          count++;
          fields_update_list.fields = record_update_list;
          // record_update_list = {}; // 写入空记录
          records_update_list.push({ "fields": record_update_list });
          record_update_list = {};
          fields_update_list = {};
        }
      }
    }
    // console.log(records_update_list);
    let new_records_update_list = grouping(records_update_list, 5000);

    // console.log(new_records_update_list);
    const date1: any = new Date();
    for (let ii = 0; ii < new_records_update_list.length; ii++) {
      uiBuilder.showLoading(`正在写入第 ` + String(ii + 1) + ` / ` + new_records_update_list.length + ` 页的 ` + new_records_update_list[ii].length + ` 条记录`);
      const date2: any = new Date();
      await table_target.addRecords(new_records_update_list[ii]);

      // 延迟代码
      if (ii < new_records_update_list.length - 1) {
        await new Promise((resolve) => {
          setTimeout(() => {
            resolve("延迟结束");
          }, 3000);
        })
      }

      const date3: any = new Date();
      console.log("第" + String(ii + 1) + "次写入时长：", String((date3 - date2) / 1000));
    }
    const date4: any = new Date();
    console.log("总写入时长：", String((date4 - date1) / 1000));

    // 隐藏加载提示
    uiBuilder.hideLoading();
    uiBuilder.message.success("数据全部写入完成");

  });
}

// 分组函数
function grouping(array: any, subGroupLength: any) {
  let index = 0;
  let newArray = [];
  while (index < array.length) {
    newArray.push(array.slice(index, index += subGroupLength));
  }
  return newArray;
}