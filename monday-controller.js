/*
  Monday.com App for Pelephone

  (c) Arthur Eyal Aminov / arthur.aminov@entrypoint.org.il
*/

const mondayService = require('../services/monday-service');
const dateFormat = require('dateformat');
const fs = require(`fs`);
const crypto = require("../services/crypto");
const funcs = require('../services/functions');

// אדמינים
let adminBoard = {
  boardId: 1446023861,
  columnIds: {
    "mata_manager": "people1__1"
  },
  rakiaColumnIds: {
    "GRID_NUM": "name",
    "WAZE": "link__1",
    "SITE_NAME": "text",
    "STATUS": "status__1",
    "ADDRESS": "text0__1",
    "OWNER_NAME": "text2",
    "OWNER_ADDRESS": "text3",
    "OWNER_PHONE_NUMS": "text7",
    "OWNER_FAX_NUM": "text31",
    "OWNER_REP_NAME": "text28",
    "OWNER_REP_ADDRESS": "text5",
    "OWNER_REP_P_NUMS": "text72",
    "OWNER_REP_BIP_SUB": "dummy",
    "OWNER_REP_BIP_NUM": "dummy",
    "SITE_KEY": "text8",
    "PERMITS": "text18",
    "SITE_ACCESS": "text6",
    "REMARKS": "text00",
    "MAHOZ": "color__1",
    "TECH_TYPE": "dummy",
    "LAT": "text1",
    "LON": "dup__of_lat",
    "IN_CHARGE": "dummy",//"people__1",
    "TOREN_RELATION": "status3__1",
    "SECURITY_REMARKS": "text__1"
  },
  dmColumnIds: {
    "ishurTifus": "files",
    "dohTahzuka": "files5",
    "asMadeWord": "files__1",
    "asMadeImage": "files3__1",
    "f2p": "files8__1",
    "f5": "files84__1",
    "toranPhotos": "files9",
    "toranPhotosDate": "date__1"
  },
  tvColumnIds: {
    "ROW_ID": "dummy",
    "TOREN_RELATION": "dummy",
    "TOREN_FORM": "dummy",
    "ADMIN_NUM": "name",  // מס' אדמין
    "TEAM_AREA": "dummy",
    "MANAGED_BY": "dummy",
    "LOCATION_NAME": "text", // שם אתר
    "RELATION_TO": "dummy",
    "LTE_ANT_OPT_ECN": "dummy",
    "REGION_MANAGER": "dummy",
    "SEND_DATE": "dummy",
    "WF_STATUS": "dummy",
    "WF_STATUS_DATE": "dummy",
    "STATUS_LTE": "dummy",
    "USERS": "dummy",
    "STATUS_4X4": "dummy",
    "CLASTER_2": "dummy",
    "IRYA": "dummy",
    "PRB96": "dummy",
    "YPRB": "dummy",
    "ZEL_KDAM": "dummy",
    "STATUS_700": "dummy",
    "BUZA_ANT_700": "dummy",
    "MEUKAV_KDAM": "dummy",
    "WIND_LOAD_EXIST_1": "dummy",
    "WIND_LOAD_APPROVED_1": "dummy",
    "WIND_LOAD_EXIST_2": "dummy",
    "WIND_LOAD_APPROVED_2": "dummy",
    "WIND_LOAD_EXIST_3": "dummy",
    "WIND_LOAD_APPROVED_3": "dummy",
    "WIND_LOAD_EXIST_5": "dummy",
    "WIND_LOAD_APPROVED_6": "dummy",
    "NP_DISTRIBUTION_DATE": "dummy",
    "HEAROT_MEUKAV": "dummy",
    "ISHUR_MAHIR": "dummy",
    "ISHUR_SHATAP": "dummy",
    "HEAROT_HOTS": "dummy",
    "SIUM_WPNIM": "dummy",
    "SIUM_WHUTS": "dummy",
    "HAVARA_KRINA": "dummy",
    "ICHS_HAKAMA": "dummy",
    "SHUCHAR_TICHNUN": "dummy",
    "STATUS_HARKASHA": "dummy",
    "WORK_DATE": "dummy",
    "CAT_NAME": "dummy",
    "SIUM_MAGBER_700": "dummy",
    "ON_AIR_700": "dummy",
    "ECN_NUM_700": "dummy",
    "STATUS_3500": "dummy",
    "ON_AIR_2600": "dummy",
    "SIUM_MAGBER_2600": "dummy",
    "SIUM_3500": "dummy",
    "ON_AIR_3500": "dummy",
    "ISHUR_YISUM_3500": "dummy",
    "IS_3_5_GHZ": "dummy",
    "DATE_PLANNED": "dummy",
    "TRAFFIC_LIGHT": "dummy",
    "BUZA_ANT_3500": "dummy",
    "BORDER_700": "dummy",
    "SECTORS_CONF": "dummy",
    "MATA_REGION": "dummy",
    "HEAROT_DATE_PLANNED": "dummy",
    "CONNECTED_USERS": "dummy",
    "EXECUTION_PRIORITY": "dummy",
    "ON_AIR_1800": "dummy",
    "MIX_MODE": "dummy",
    "SHATAP_PRIORITY": "dummy",
    "IS_800": "dummy",
    "HEAROT_LETEUM": "dummy",
    "SECTOR_CONF_2600": "dummy",
    "ANT_3_5_AND_COUNT": "dummy",
    "ECN_NUM_6651": "dummy",
    "BUZA_6651": "dummy",
    "IS_MIX_MODE_5G": "dummy",
    "HEAROT_BITZUA": "dummy",
    "SITE_NUM_OPTION": "dummy",
    "REGION_ID": "dummy",
    "MENAHEL_MERHAV": "dummy",
    "SIUM_BINUI": "dummy",
    "NIDRASH_CONS": "dummy",
    "NISHLACH_CONS": "dummy",
    "ISHUR_CONS": "dummy",
    "STAT_CONST": "dummy",
    "STATUS": "status__1",  // סטטוס אתר
    "BTS_NUMBERS": "dummy",
    "SPECIAL_MEANS": "dummy",
    "LTE_BTS_NUMBERS": "dummy",
    "ECN_NUM": "dummy",
    "MENAHEL_HAKAMA": "dummy",
    "ECN_NUM_3_5": "dummy",
    "ECN_LINK_700": "dummy",
    "STATUS_MOD_2100": "dummy",
    "STATUS_MOD_850": "dummy",
    "KABLAN_TOREN": "dummy",
    "NIDRASH_SHATAP": "dummy",
    "HEAROT_SHATAP": "dummy",
    "HEAROT_BINUI_H_3500": "dummy",
    "NIDRASH_HUTS": "dummy",
    "HEAROT_RAMA": "dummy",
    "CHANGE_TO_6630": "dummy",
    "SIV": "dummy",
    "NIDRASH_BINUI_H_3500": "dummy",
    "SIUM_BINUI_H_3500": "dummy",
    "TRAFFIC_ARR": "dummy",
    "SURVEY_REQUIRED": "dummy",
    "MAGBER_1800": "dummy",
    "ECN_NUM_800": "dummy",
    "IS_800MHZ": "dummy",
    "STATUS_800": "dummy",
    "MW_BEZEQ_REPLACE": "dummy",
    "MIXMODE_DOABLE": "dummy",
    "NIDRASH_BINUI_PNIM": "dummy",
    "WORK_ORDER": "dummy",
    "ACCUMULATOR_REQ_SPPLY": "dummy",
    "ACCUMULATOR_REQ_BTTR": "dummy",
    "ACCUMULATOR_SPPLY_TYPE": "dummy",
    "ACCUMULATOR_SPPLY_COUNT": "dummy",
    "ACCUMULATOR_BTTR_TYPE": "dummy",
    "ACCUMULATOR_BTTR_COUNT": "dummy",
    "ECN_LINK_800": "dummy",
    "ECN_LINK_3_5": "dummy",
    "ECN_LINK_6651": "dummy"
  }
};

// ניהול תקלות
let faultsBoard = {
  boardId: 1455166490,
  autoNumberMask: 'MND0000000000'
};

let boardIds = {
  '1446023861': adminBoard,
  '1455166490': faultsBoard
};

const dmFileTypes = {
  304: "asMadeWord",
  311: "asMadeImage",
  215: "f2p", // f5
  495: "dohTahzuka",
  496: "ishurTifus",
  411: "toranPhotos"
};

const convert2null = 'לא מוגדר';

const folders = {
  rakia2monday: {
    path: '/monday_app/Rakia2Monday/',
    filename: 'AdminDataTO_CHANGE.json',
    backupFolder: 'history/'
  },
  dm2monday: {
    path: '/monday_app/DM2Monday/',
    filename: '',
    backupFolder: 'history/'
  },
  monday2dm: {
    path: '/monday_app/Monday2DM/',
    filename: '',
    backupFolder: 'backup/'
  },
  tv2monday: {
    path: '/monday_app/TV2Monday/',
    filename: 'prj_49.json',
    backupFolder: 'TV2MondayPrevious/'
  },
  monday2tv: {
    path: '/monday_app/Monday2TV/',
    filename: '',
    backupFolder: 'backup/'
  }
};

async function setAutoNumberOnName(req, res)
{
  console.log('setAutoNumberOnName');

  try {
    const { shortLivedToken } = req.session;
    const { boardId, itemId } = req.body.payload.inputFields;

    //console.log(req.body);

    let columnId = "name"
    let autoNumberMask = boardIds[boardId].autoNumberMask;
    let numPrefix = autoNumberMask.split(/(\d+)/)[0];

    // get max number from board
    let objValue = await mondayService.getColumnMaxMinValue(shortLivedToken, boardId, columnId, numPrefix);
    let maxValue = objValue.value;

    console.log('found max number: ' + maxValue);

    if (!maxValue) maxValue = autoNumberMask;
    else {
      let maxValue_r = maxValue.split(/(\d+)/);
      let numLength = maxValue_r[1].length;

      if (!isNaN(Number(maxValue_r[1]))) {

        if (itemId == objValue.itemId) {
          console.log('item already has auto number');
          return res.status(200).send({ result: 'success' });
        }

        maxValue = maxValue_r[0] + ((++maxValue_r[1])+'').padStart(numLength, '0');
      }
    }

    console.log('new max number: ' + maxValue);

    // update new max number
    let t = { id: itemId, columns: {} };
    t.columns[columnId] = maxValue;

    await mondayService.updateColumns(shortLivedToken, boardId, [t]);

    return res.status(200).send({ result: 'success' });
  } catch (err) {
    console.error(err);
    return res.status(500).send({ message: 'internal server error' });
  }
}

async function setMataManagerOnFaultsBoard(req, res)
{
  console.log('setMataManagerOnFaultsBoard');

  try {
    const { shortLivedToken } = req.session;
    const { boardId, itemId, adminNumColumnId, mataManagerColumnId } = req.body.payload.inputFields;

    //console.log(req.body);

    // get admin num from faults board
    const faultsItem = await mondayService.getItemData(shortLivedToken, itemId, [adminNumColumnId]);
    //console.log(faultsItem);

    let adminNum = faultsItem.column_values[0].display_value;

    if (adminNum != '') {
      // get mata manager from admins board
      const adminItem = await mondayService.getAllSpecificItems(shortLivedToken, adminBoard.boardId, [adminNum], [adminBoard.columnIds.mata_manager], 1);
      //console.log(adminItem);

      // update mata manager in faults board
      let personsAndTeams = JSON.parse(adminItem[0].column_values[0].value)?.personsAndTeams;

      if (personsAndTeams) {
        let t = { id: itemId, columns: {} };
        t.columns[mataManagerColumnId] = { personsAndTeams: personsAndTeams };

        await mondayService.updateColumns(shortLivedToken, boardId, [t]);
      }
    } else {
      console.log('Empty Admin Num');
      return res.status(200).send({ result: 'Failed. Empty Admin Num' });
    }

    return res.status(200).send({ result: 'success' });
  } catch (err) {
    console.error(err);
    return res.status(500).send({ message: 'internal server error' });
  }
}

async function dm2monday()
{
  console.log("dm2monday");

  // monday token
  const token = crypto.decrypt(process.env.MONDAY_TOKEN_ENC);

  try {
    let date = new Date();
    let files2monday_r = [];
    let adminNums_r = [];
let i = 0;
    // get list of files
    fs.readdirSync(folders.dm2monday.path).every(file => {
      if (file.indexOf('-') != -1) {
        const [fileAdminNum, fileType, fileDateExt] = file.split('-');

        if (fileDateExt && fileDateExt.indexOf('.') != -1) {
          const [fileDate, fileExt] = fileDateExt.split('.');

          const fileStats = fs.statSync(folders.dm2monday.path + file);

          //if (dateFormat(fileStats.mtime, 'yyyymmdd') === dateFormat(date, "yyyymmdd")) {
            //console.log(file);

            // map files to columns
            let t = { id: '', name: fileAdminNum, fileType: fileType, fileName: file, columns: {} };
            t.columns[adminBoard.dmColumnIds[dmFileTypes[fileType]]] = folders.dm2monday.path + file;
console.log(t);
            files2monday_r.push(t);

            if (adminNums_r.indexOf(fileAdminNum) == -1) {
              adminNums_r.push(fileAdminNum);
			  //if (adminNums_r.length > 100) return false;
            }
          //}
        }
      }
	  return true;
    });

    //console.log(files2monday_r);
    console.log(adminNums_r.join(","));

    if (adminNums_r?.length) {

	// create new history folder
	let historyFolder = folders.dm2monday.path + folders.dm2monday.backupFolder + dateFormat(date, "yyyy-mm-dd") + '/';
	if (!fs.existsSync(historyFolder)) {
		fs.mkdirSync(historyFolder);
	}

      // get all existing items for update
      let items_r = await mondayService.getAllSpecificItems(token, adminBoard.boardId, adminNums_r, ["name"]);
      //console.log(items_r);

      // map item ids
      for (let f = 0; f < files2monday_r?.length; f++) {
        let file = files2monday_r[f];
		let found = false;
        for (let i = 0; i < items_r?.length; i++) {
          let item = items_r[i];

          if (file.name == item.name) {
			console.log(item.name);
			  found=true;
            let columnId = adminBoard.dmColumnIds[dmFileTypes[file.fileType]];

            let res = await mondayService.uploadFileToItem(token, item.id, columnId, file.columns[columnId]);
			if (res?.account_id) {
				files2monday_r[f].id = item.id;
				// move file to history folder
        fs.rename(file.columns[columnId], historyFolder + file.fileName, function(err) {
          if (err) {
            console.log(err);
          } else {
            console.log('file "' + file.fileName + '" moved to hitory folder');
          }
        });
			}
			console.log(files2monday_r[f]);
          }
        }
		if (!found) {
		// move file to no-admin-files folder
        fs.rename(folders.dm2monday.path + file.fileName, folders.dm2monday.path + 'no-admin-files/' + file.fileName, function(err) {
          if (err) {
            console.log(err);
          } else {
            console.log('file "' + file.fileName + '" moved to no-admin-files folder');
          }
        });
			console.log('No ADMIN found for file:');
			console.log(file);
		}
      }

      //console.log(files2monday_r);
      console.log('All files uploaded');
    } else {
      console.log('No files found');
    }

  } catch (err) {
    console.log(err);
  }
}

async function monday2dm()
{
  console.log("monday2dm");

  // monday token
  const token = crypto.decrypt(process.env.MONDAY_TOKEN_ENC);

  try {
    let date = new Date();

    // get all items from board with file columns and assets
    let columnIds_r = Object.values(adminBoard.dmColumnIds);
    let rulesCompareValues_r = Array.from({length: columnIds_r.length}, (v, k) => [""]);
    let rulesOperators_r = Array.from({length: columnIds_r.length}, (v, k) => "is_not_empty");

    let items_r = await mondayService.getAllItemsDataByRules(token, adminBoard.boardId, columnIds_r, ["file"], 100, columnIds_r, rulesCompareValues_r, rulesOperators_r, "or", false, false, true);

    console.log(items_r);

    // items
    for (let i = 0; i < items_r?.length; i++) {
      let item = items_r[i];
      let assets_r = item.assets;
      let columns_r = item.column_values;

      // columns
      for (let c = 0; c < columns_r?.length; c++) {
        let column = columns_r[c];
        let files = JSON.parse(column.value);

        if (files?.files) {
          let columnName = Object.keys(adminBoard.dmColumnIds).find(key => adminBoard.dmColumnIds[key] === column.id);
          let fileType = Object.keys(dmFileTypes).find(key => dmFileTypes[key] === columnName);

          // item assets
          for (let a = 0; a < assets_r?.length; a++) {
            let asset = assets_r[a];

            // column files
            for (let f = 0; f < files.files.length; f++) {
              let file = files.files[f];

              // match file ids to get the right public url
              if (asset.id == file.assetId) {
                let fileExt = file.name.substring(file.name.lastIndexOf('.'));
                let filename = item.name + '-' + fileType + '-' + dateFormat(new Date(asset.created_at), 'yyyymmdd') + fileExt;

                if (!fs.existsSync(folders.monday2dm.path + filename)) {
                  await funcs.downloadFile(asset.public_url, folders.monday2dm.path, filename);
                  //console.log(item.name + ' ' + file.name + ' ' + column.column.title + ' ' + filename);
                }
              }
            }
          }
        }
      }
    }

    console.log('finished monday2dm');

  } catch (err) {
    console.log(err);
  }
}

async function rakia2monday()
{
  console.log("rakia2monday");

  // monday token
  const token = crypto.decrypt(process.env.MONDAY_TOKEN_ENC);

  let rakiaRows_r = [];
  let rakiaNames_r = [];
  let date = new Date();

  try {
    // read rakia file
    let filepath = folders.rakia2monday.path;
    let filename = folders.rakia2monday.filename.replace('TO_CHANGE', dateFormat(date, 'ddmmyyyy'));

    if (!fs.existsSync(filepath + filename)) {
      console.log('File does not exists: ' + filepath + filename);
    } else {
      let jsonData = fs.readFileSync(filepath + filename);

      if (jsonData == '') {
        console.log('Empty file: ' + filepath + filename);
      } else {
        fileObj = JSON.parse(jsonData);

        // map json items to rakiaRows object
        for (let o = 0; o < fileObj.length; o++) {
          let obj = fileObj[o];
          let rakiaRows = {};

          // map json data to board columns
          Object.entries(obj).forEach(entry => {
            let [key, val] = entry;
            let columnId = adminBoard.rakiaColumnIds[key];

            if (val) val = val.replace("\u001c", "");

            if (columnId != 'dummy') {
              if (key == 'WAZE') {
                val = { "url": val, "text": val };
              } else if (key == 'IN_CHARGE') {  // people column must be existing user in monday
                if (val == convert2null) val = null;
              }

              rakiaRows[columnId] = val;
            }
          });

          rakiaRows_r.push(rakiaRows);
          rakiaNames_r.push(rakiaRows_r[o].name);
        }

        console.log(rakiaRows_r);

        /// MONDAY
        await mondayService.getItemsAndUpsert(token, adminBoard.boardId, rakiaRows_r, rakiaNames_r);

        // move file to backup folder
        fs.rename(filepath + filename, filepath + folders.rakia2monday.backupFolder + filename, function(err) {
          if (err) {
            console.log(err);
          } else {
            console.log('file moved to history folder');
          }
        });
      }
    }
  } catch (err) {
    console.log(err);
  }
}

async function monday2tv()
{
  console.log("monday2tv");

  // monday token
  const token = crypto.decrypt(process.env.MONDAY_TOKEN_ENC);

  let tvColumnNames_r = [];
  let tvRows_r = [];

  try {
    // get all items from board
    let items_r = await mondayService.getAllItemsData(token, adminBoard.boardId);

    // create object suitable for TV file format for safe in pelephone
    for (let i = 0; i < items_r.length; i++) {
      let item = items_r[i];
      let columns_r = item.column_values;
      let tvRowsData_r = [];

      let colName = Object.keys(adminBoard.tvColumnIds).find(key => adminBoard.tvColumnIds[key] === 'name');

      if (colName != undefined) {
        if (!i) tvColumnNames_r.push(colName);
        tvRowsData_r.push(item.name);
      }

      for (let c = 0; c < columns_r.length; c++) {
        let column = columns_r[c];
        colName = Object.keys(adminBoard.tvColumnIds).find(key => adminBoard.tvColumnIds[key] === column.id);

        if (colName != undefined) {
          if (!i) tvColumnNames_r.push(colName);
          tvRowsData_r.push(column.text);
        }
      }

      tvRows_r.push(tvRowsData_r);
    }

    let fileObj = {
      column_names: tvColumnNames_r,
      rows: tvRows_r
    }

    let date = new Date();
    let fileData = JSON.stringify(fileObj);
    fs.writeFileSync('/monday_app/TV_OUT/prj_49_' + dateFormat(date, 'yyyymmdd') + '.json', fileData);

    console.log('created TV file to move to safe');

    // TODO: move file to safe
  } catch (err) {
    console.log(err);
  }
}

async function tv2monday()
{
  console.log("tv2monday");

  // monday token
  const token = crypto.decrypt(process.env.MONDAY_TOKEN_ENC);

  let tvRows_r = [];
  let tvNames_r = [];
  let tvUpdate_r = [];
  let tvCreate_r = [];

  try {
    let filepath = folders.tv2monday.path;
    let filename = folders.tv2monday.filename;

    let rawdata = fs.readFileSync(filepath + filename);

    if (rawdata != '') {
      fileObj = JSON.parse(rawdata);

      // map json items to tvRows object
      for (let r = 0; r < fileObj.rows.length; r++) {
        let row = fileObj.rows[r];
        let tvRows = {};

        for (let c = 0; c < row.length; c++) {
          //tvRows[fileObj.column_names[c]] = row[c];
          // map json data to board columns

          if (adminBoard.tvColumnIds[fileObj.column_names[c]] != 'dummy') {
            tvRows[adminBoard.tvColumnIds[fileObj.column_names[c]]] = row[c];
          }
        }

        tvRows_r.push(tvRows);
        tvNames_r.push(tvRows_r[r].name);
      }

      console.log(tvRows_r);

      /// MONDAY

      // get all existing items for update
      let existingItems_r = await mondayService.getAllSpecificItems(token, adminBoard.boardId, tvNames_r);

      for (let r = 0; r < tvRows_r.length; r++) {
        let t = { id: '', columns: {} };

        for (let e = 0; e < existingItems_r.length; e++) {
          if (existingItems_r[e].name == tvRows_r[r].name) {
            t = { id: existingItems_r[e].id, columns: tvRows_r[r] };
            tvUpdate_r.push(t);
            break;
          }
        }

        if (t.id == '') {
          tvCreate_r.push(tvRows_r[r]);
        }
      }

      // update existing items
      console.log('items to update: ' + tvUpdate_r.length);
      await mondayService.updateColumns(token, adminBoard.boardId, tvUpdate_r);

      // create new items
      console.log('items to create: ' + tvCreate_r.length);
      await mondayService.createItems(token, adminBoard.boardId, tvCreate_r);

      // move file to backup folder
      fs.rename(filepath + filename, filepath + folders.tv2monday.backupFolder + filename, function(err) {
        if (err) {
          console.log(err);
        } else {
          console.log('file moved to backup folder');
        }
      });
    }
  } catch (err) {
    console.log(err);
  }
}

module.exports = {
  setAutoNumberOnName,
  setMataManagerOnFaultsBoard,
  rakia2monday,
  dm2monday,
  monday2dm,
  tv2monday,
  monday2tv
};
