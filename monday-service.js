/*
  Monday.com API Version: 2023-10

  (c) Arthur Eyal Aminov / arthur.aminov@entrypoint.org.il
*/

const mondaySdk = require('monday-sdk-js-proxy');
const monday = mondaySdk();
const fs = require(`fs`);
const fetch = require("node-fetch");
const https = require(`https`);
const funcs = require('./functions');
const { HttpsProxyAgent } = require('https-proxy-agent'); 

monday.setApiVersion('2023-10');

const proxyAgent = new HttpsProxyAgent(process.env.PROXY_URL);

const getItemData = async (token, itemIds, columnIds) => {
  try {
    monday.setToken(token);

    let itmIds = itemIds?.length ? `(ids: [` + itemIds.join(',') + `])` : `(ids: ${itemIds})`;
    let colIds = columnIds?.length ? `(ids: ["` + columnIds.join('","') + `"])` : '';

    const query = `query {
        items ${itmIds} {
          id name
          column_values ${colIds} { id text type value column { title, settings_str } ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
        }
      }`;

    const options = { proxyAgent: proxyAgent };
    const response = await monday.api(query, options);

    console.log(response);

    let items = response.data.items;

    return items.length > 1 ? items : items[0];
  } catch (err) {
    console.error(err);
  }
};

// get all board items by query params and rules
const getAllItemsDataByRules = async (token, boardId, columnIds = [], columnTypes = [], pageLimit = 100, rulesColumndIds = [], rulesCompareValues = [], rulesOperators = [], queryParamsOperator = "and", getGroups = false, getSubitems = false, getAssets = false) => {
  console.log('getAllItemsDataByRules');

  try {
    monday.setToken(token);

    let items_r = [];
    let groups = getGroups ? `group { id title color }` : '';
    let colTypes = columnTypes?.length ? `types: [` + columnTypes.join(',') + `]` : '';
    let colIds = columnIds?.length ? `ids: ["` + columnIds.join('","') + `"]` : '';
    let columnParams = colTypes || colIds ? `(${colTypes} ${colIds})` : '';
    let subitems = getSubitems ? `subitems { id name column_values { id column { title } text value }}` : '';
    //let assets = getAssets ? `assets (assets_source: all) { name public_url url url_thumbnail }` : '';
    let assets = getAssets ? `assets { id name public_url url url_thumbnail created_at }` : '';

    let rules_r = [];

    for (let r = 0; r < rulesColumndIds?.length; r++) {
      let rule = rulesColumndIds[r];
      let ruleCompareValues = rulesCompareValues[r];
      rules_r.push(`{column_id: "${rule}", compare_value: ["` + ruleCompareValues.join('","') + `"], operator: ${rulesOperators[r]}}`);
    }

    queryParamsOperator = queryParamsOperator == 'or' ? 'operator: or' : '';
    let query_params = rules_r?.length ? `query_params: { ${queryParamsOperator} rules: [` + rules_r.join(",") + "]}": '';

    let query = `query {
      boards (ids: ${boardId}) {
         items_page (limit:${pageLimit} ${query_params}) {
           cursor
           items {
            id name
            ${groups}
            column_values ${columnParams} { id text type value column { title } ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
            ${subitems}
            ${assets}
           }
          }
       }
     }`;

    const options = { proxyAgent: proxyAgent };
    const firstPage = await monday.api(query, options);
 
    items_r = firstPage.data.boards[0].items_page.items;
    let cursor = firstPage.data.boards[0].items_page.cursor;
    
    while (cursor) {
      // loop will stop when cursor is null
      query = `query {
        next_items_page (limit:${pageLimit}, cursor: "${cursor}") {
          cursor
          items {
            id name
            ${groups}
            column_values ${columnParams} { id text type value column { title } ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
            ${subitems}
            ${assets}
           }
        }
      }`;

      const nextPage = await monday.api(query, options);

      console.log(nextPage);

      if (nextPage?.error_code) {
        console.error(nextPage);
        if (nextPage.error_code == 'ComplexityException') {
          // parse seconds from error message
          let errorMsg = nextPage.errors[0];
          let regex = /in (\d+) seconds/;
          let seconds = Number(regex.exec(errorMsg)[1]) + 1;  // addig 1 seconds to be sure

          // wait parsed seconds before continuing
          console.log(`waiting ${seconds} seconds before continuing...`);
          await funcs.sleep(seconds * 1000);
        }
      } else {
        items_r = items_r.concat(nextPage.data.next_items_page.items);
        cursor = nextPage.data.next_items_page.cursor;
      }
    }

    console.log("All items retrieved");

    return items_r;
  } catch (err) {
    console.error(err);
  }
};

// get all board items and slelected/all columns
const getAllItemsData = async (token, boardId, columnIds = [], columnTypes = [], getGroups = false, getSubitems = false, getAssets = false) => {
  console.log('getAllItemsData');

  try {
    monday.setToken(token);

    let items_r = [];
    let groups = getGroups ? `group { id title color }` : '';
    let colTypes = columnTypes?.length ? `types: [` + columnTypes.join(',') + `]` : '';
    let colIds = columnIds?.length ? `ids: ["` + columnIds.join('","') + `"]` : '';
    let columnParams = colTypes || colIds ? `(${colTypes} ${colIds})` : '';
    let subitems = getSubitems ? `subitems { id name column_values { id column { title } text value }}` : '';
    //let assets = getAssets ? `assets (assets_source: all) { name public_url url url_thumbnail }` : '';
    let assets = getAssets ? `assets { id name public_url url url_thumbnail }` : '';
    let limit = 100;

    const options = { proxyAgent: proxyAgent };
    const firstPage = await monday.api(
      `query {
        boards (ids: ${boardId}) {
           items_page (limit:${limit}) {
             cursor
             items {
              id name
              ${groups}
              column_values ${columnParams} { id text type value column { title } ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
              ${subitems}
              ${assets}
             }
            }
         }
       }`,
       options
    );

    items_r = firstPage.data.boards[0].items_page.items;
    let cursor = firstPage.data.boards[0].items_page.cursor;
    
    while (cursor) {
      // loop will stop when cursor is null
      const nextPage = await monday.api(
        `query {
          next_items_page (limit:${limit}, cursor: "${cursor}") {
            cursor
            items {
              id name
              ${groups}
              column_values ${columnParams} { id text type value column { title } ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
              ${subitems}
              ${assets}
             }
          }
        }`,
        options
      );

      console.log(nextPage);

      if (nextPage?.error_code) {
        console.error(nextPage);
        if (nextPage.error_code == 'ComplexityException') {
          // parse seconds from error message
          let errorMsg = nextPage.errors[0];
          let regex = /in (\d+) seconds/;
          let seconds = Number(regex.exec(errorMsg)[1]) + 1;  // addig 1 seconds to be sure

          // wait parsed seconds before continuing
          console.log(`waiting ${seconds} seconds before continuing...`);
          await funcs.sleep(seconds * 1000);
        }
      } else {
        items_r = items_r.concat(nextPage.data.next_items_page.items);
        cursor = nextPage.data.next_items_page.cursor;
      }
    }

    console.log("All items retrieved");

    return items_r;
  } catch (err) {
    console.error(err);
  }
};

// get all specific items
const getAllSpecificItems = async (token, boardId, itemName_r, columnIds = [], limit = 500, operator = 'any_of') => {
  console.log('getAllSpecificItems');

  try {
    monday.setToken(token);

    let colIds = columnIds?.length ? `(ids: ["` + columnIds.join('","') + `"])` : '';

    let items_r = [];

    const options = { proxyAgent: proxyAgent };
    const firstPage = await monday.api(
      `query {
        boards (ids: ${boardId}) {
           items_page (limit:${limit} query_params: {rules: [{column_id: "name", compare_value: ["` + itemName_r.join('","') + `"], operator: ${operator}}]}) {
             cursor
             items {
              id name
              column_values ${colIds} { id text type value column { title } ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
             }
            }
         }
       }`,
       options
    );

    items_r = firstPage.data.boards[0].items_page.items;
    let cursor = firstPage.data.boards[0].items_page.cursor;
    
    while (cursor) {
      // loop will stop when cursor is null
      const nextPage = await monday.api(
        `query {
          next_items_page (limit:${limit}, cursor: "${cursor}") {
            cursor
            items {
              id name
              column_values ${colIds} { id text type value column { title } ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
             }
          }
        }`,
        options
      );

      console.log(nextPage);

      if (nextPage?.error_code) {
        console.error(nextPage);
        if (nextPage.error_code == 'ComplexityException') {
          // parse seconds from error message
          let errorMsg = nextPage.errors[0];
          let regex = /in (\d+) seconds/;
          let seconds = Number(regex.exec(errorMsg)[1]) + 1;  // addig 1 seconds to be sure

          // wait parsed seconds before continuing
          console.log(`waiting ${seconds} seconds before continuing...`);
          await funcs.sleep(seconds * 1000);
        }
      } else {
        items_r = items_r.concat(nextPage.data.next_items_page.items);
        cursor = nextPage.data.next_items_page.cursor;
      }
    }

    console.log("All items retrieved");

    return items_r;
  } catch (err) {
    console.error(err);
  }
};

const updateColumns = async (token, boardId, items_r) =>
{
  console.log("updateColumns");

  try {
    monday.setToken(token);

    let updateColumnsMutation = '';
    let bulkSize = 50;
    const options = { proxyAgent: proxyAgent };

    for (let i = 0; i < items_r.length; i++) {
      let item = items_r[i];
      let columnValuesUpdate = JSON.stringify(JSON.stringify(item.columns));

      updateColumnsMutation += `updateColumns${i}: change_multiple_column_values(item_id: ${item.id}, board_id: ${boardId}, column_values: ${columnValuesUpdate}, create_labels_if_missing: true) { id name }`;

      if ((i && (i % bulkSize == 0)) || i == items_r.length - 1)
      {
        updateColumnsMutation = `mutation { ${updateColumnsMutation} }`;

        console.log(updateColumnsMutation);

        let response = await monday.api(updateColumnsMutation, options);

        updateColumnsMutation = '';
        console.log(response);
      }
    }
  } catch (err) {
    console.error(err);
  }
}

const createItems = async (token, boardId, items_r) =>
{
  console.log("createItems");

  try {
    monday.setToken(token);

    let createItemsMutation = '';
    let bulkSize = 50;
    const options = { proxyAgent: proxyAgent };

    for (let i = 0; i < items_r.length; i++) {
      let item = items_r[i];
      let columnValues = JSON.stringify(JSON.stringify(item));

      createItemsMutation += `createItems${i}: create_item(board_id: ${boardId}, item_name: "${item.name}", column_values: ${columnValues}, create_labels_if_missing: true) { id name }`;

      if ((i && (i % bulkSize == 0)) || i == items_r.length - 1)
      {
        createItemsMutation = `mutation { ${createItemsMutation} }`;

        console.log(createItemsMutation);

        let response = await monday.api(createItemsMutation, options);

        createItemsMutation = '';

        if (response.error_code != '' && response.error_data) {
          console.error(response);
          break;
        }

        console.log(response);
      }
    }
  } catch (err) {
    console.error(err);
  }
}

async function getItemsAndUpsert(token, boardId, rows_r, names_r)
{
  let update_r = [];
  let create_r = [];

  // get all existing items for update
  let existingItems_r = await getAllSpecificItems(token, boardId, names_r);

  for (let r = 0; r < rows_r.length; r++) {
    let t = { id: '', columns: {} };

    for (let e = 0; e < existingItems_r.length; e++) {
      if (existingItems_r[e].name == rows_r[r].name) {
        t = { id: existingItems_r[e].id, columns: rows_r[r] };
        update_r.push(t);
        break;
      }
    }

    if (t.id == '') {
      create_r.push(rows_r[r]);
    }
  }

  // update existing items
  console.log('items to update: ' + update_r.length);
  await updateColumns(token, boardId, update_r);

  // create new items
  console.log('items to create: ' + create_r.length);
  await createItems(token, boardId, create_r);
}

const getColumnMaxMinValue = async (token, boardId, columnId, compare_value = null, operator = 'starts_with', max = true) => {
  try {
    monday.setToken(token);

    let orderDir = max ? 'desc' : 'asc';
    compare_value = compare_value ? `rules: [{column_id: "${columnId}", compare_value: ["${compare_value}"], operator: ${operator}}],` : '';

    const query = `query {
        boards (ids: ${boardId}) {
           items_page (limit:1 query_params: {${compare_value} order_by:[{column_id: "${columnId}", direction: ${orderDir}}]}) {
             items {
              id name
              column_values (ids: "${columnId}") { id text type value ...on MirrorValue { display_value } ...on DependencyValue { display_value } ...on BoardRelationValue { display_value }}
             }
            }
         }
       }`;
    
    const options = { proxyAgent: proxyAgent };
    const response = await monday.api(query, options);

    console.log(response);

    let valObj = response.data.boards[0].items_page.items[0]?.column_values[0];
    let val = '';

    if (valObj?.display_value) {
      val = valObj.display_value;
    } else if (valObj?.text) {
      val = valObj.text;
    } else if (valObj?.value) {
      val = valObj?.value;
    } else {
      val = response.data.boards[0].items_page.items[0]?.name;
    }

    return {
      itemId: response.data.boards[0].items_page.items[0]?.id,
      value: val
    }

  } catch (err) {
    console.error(err);
  }
};

const uploadFileToItem = async (token, itemId, columnId, filepath) =>
{
  console.log("uploadFileToItem");
  let response = null;

  try {
    let query = `mutation ($file: File!) { add_file_to_column (file: $file, item_id: ${itemId}, column_id: "${columnId}") { id name }}`;

    // set URL and boundary
    let url = "https://api.monday.com/v2/file";
    let boundary = "xxxxxxxxxx";
    let filename = filepath.substring(filepath.lastIndexOf("/") + 1);

    let content = fs.readFileSync(filepath);

    // construct query part
    let data = "--" + boundary + "\r\n";
    data += "Content-Disposition: form-data; name=\"query\"; \r\n";
    data += "Content-Type:application/json\r\n\r\n";
    data += "\r\n" + query + "\r\n";

    // construct file part
    data += "--" + boundary + "\r\n";
    data += "Content-Disposition: form-data; name=\"variables[file]\"; filename=\"" + filename + "\"\r\n";
    data += "Content-Type:application/octet-stream\r\n\r\n";

    var payload = Buffer.concat([
      Buffer.from(data, "utf8"),
      new Buffer.from(content, 'binary'),
      Buffer.from("\r\n--" + boundary + "--\r\n", "utf8"),
    ]);

    // construct request options
    var options = {
      method: 'post',
      headers: {
        "Content-Type": "multipart/form-data; boundary=" + boundary,
        "Authorization" : token
      },
      body: payload,
      agent: proxyAgent
    };

    // upload
	for (let r = 0; r < 3; r++) {	// retries
		response = null;
		try {
			await fetch(url, options).then((res) => {
			  if (res.status !== 200) {
				throw(res.statusText);
			  }
			  return res.json();
			}).then(async (json) => {
				response = json;
			  console.log(json);
			  if (json?.error_code) {
				console.error(json);
				if (json.error_code == 'ComplexityException') {
				  // parse seconds from error message
				  let errorMsg = json.errors[0];
				  let regex = /in (\d+) seconds/;
				  let seconds = Number(regex.exec(errorMsg)[1]) + 1;  // adding 1 second to be sure

				  // wait parsed seconds before continuing
				  console.log(`waiting ${seconds} seconds before continuing...`);
				  await funcs.sleep(seconds * 1000);
				}
			  }
			});
			break;
		} catch (err) {
			console.error(filename);
			console.error(err);
		}
	}
  } catch (err) {
	console.error(filename);
    console.error(err);
  }
  
  return response;
}

module.exports = {
  getItemData,
  getAllItemsDataByRules,
  getAllItemsData,
  getAllSpecificItems,
  updateColumns,
  createItems,
  getItemsAndUpsert,
  getColumnMaxMinValue,
  uploadFileToItem
};