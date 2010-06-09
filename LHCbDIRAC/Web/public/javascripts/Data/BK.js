var dataMngr = [];
var tableMngr = []; 
bkObject = new Object();
function initBK(reponseSelect){
  Ext.onReady(function(){
    var leftBar = bkSidebar(); // Done
    var rightBar = bkRight(); // Done
    var mainContent = bkTable(); // Done
    leftBar.on('render',leftbarFunction = function(panel){ // Add listner which will be trigered by tree load
      var loader = panel.items.items[0].loader;
      loader.on('load',loaderFunction = function(x,y,z){ // Parsing of the URL to options
        var hash = parent.location.hash;
        if(hash){
          var request = DEncode.decode(hash.substr(1));
          execRequest(request);
        }
        loader.removeListener('load',loaderFunction);
      });
      leftBar.removeListener('render',leftbarFunction);
    });
    renderInMainViewport([ leftBar, mainContent, rightBar ]);
  });
}
function bkSidebar(){
  var tree = initTree(); // BKSupport.js
  var bookmark = initBookmarkPanel(); // BKSupport.js
  var bar = sideBar(); // Lib.js
  bar.setWidth(400);
  bar.insert(0,tree);
  bar.insert(1,bookmark);
  return bar
}
function bkRight(){
  function saveButton(minWidth){
    if(!minWidth){
      minWidth = '';
    }
    var button = new Ext.Button({
      cls:"x-btn-text-icon",
      handler:function(){
        bkSaveDialog()
      },
      icon:gURLRoot+'/images/iface/save.gif',
      minWidth:minWidth,
      tooltip:'Click the button if you want to save records to a file',
      text:'Save dialog'
    });
    return button
  }
  function createField(id,label){
    var txtField = new Ext.form.TextField({
      anchor:'90%',
      fieldLabel:label,
      id:id,
      readOnly:true
    });
    return txtField;
  }
  var fSet1 = {
    autoHeight:true,
    defaultType:'textfield',
    items:[
      createField('cName','Configuration Name'),
      createField('cVersion','Configuration Version'),
      createField('simCond','Simulation Conditions'),
      createField('procPass','Processing pass'),
      createField('eType','Event Type'),
      createField('fType','File Type'),
    ],
    labelAlign:'top',
    xtype:'fieldset'
  }
  var fSet2 = {
    autoHeight:true,
    defaultType:'textfield',
    items:[
      createField('nof','Number Of Files'),
      createField('noe','Number Of Events'),
      createField('fSize','File(s) Size')
    ],
    labelAlign:'top',
    title:'Statistics',
    xtype:'fieldset'
  }
  var panel = new Ext.Panel({
    autoScroll:true,
    bbar:new Ext.Toolbar({
      items:[saveButton()]
    }),
    collapsible:false,
    id:'bkRight',
    split:true,
    region:'east',
    margins:'2 0 2 0',
    cmargins:'2 2 2 2',
    bodyStyle:'padding: 5px',
    width: 200,
    labelAlign:'top',
    minWidth: 200,
    items:[fSet1,fSet2],
    title:'Bookkeeping info',
  });
  panel.addListener('resize',function(){
    var tmpWidth = panel.getInnerWidth() - 4;
    var button = saveButton(tmpWidth);
    var bar = panel.getBottomToolbar();
    Ext.fly(bar.items.get(0).getEl()).remove();
    bar.items.removeAt(0);
    bar.insertButton(0,button);
    panel.doLayout();
  });
  return panel
}
function bkTable(){
  var record = new Ext.data.Record.create([
    {name:'Name'},
    {name:'EventStat'},
    {name:'FileSize'},
    {name:'RunNumber'},
    {name:'PhysicStat'},
    {name:'CreationDate',type:'date',dateFormat:'Y-n-j h:i:s'},
    {name:'JobStart',type:'date',dateFormat:'Y-n-j h:i:s'},
    {name:'JobEnd',type:'date',dateFormat:'Y-n-j h:i:s'},
    {name:'WorkerNode'},
    {name:'FileType'},
    {name:'EvtTypeId'}
  ]);
  var columns = [
    {header:'#',width:30,sortable:false,locked:true,renderer:function(a,b,c,rowIndex,d,ds){return pageRowNumber(ds,rowIndex)},hideable:false,fixed:true,menuDisabled:true},
    {header:'File Name',sortable:true,dataIndex:'Name',align:'left'},
    {header:'Event Stat',sortable:true,dataIndex:'EventStat',align:'left'},
    {header:'File Size',sortable:true,dataIndex:'FileSize',align:'left'},
    {header:'Run number',sortable:true,dataIndex:'RunNumber',align:'left'},
    {header:'Physics statistics',sortable:true,dataIndex:'PhysicStat',align:'left'},
    {header:'Creation Date',sortable:true,dataIndex:'CreationDate',align:'left',hidden:true},
    {header:'Job Start',sortable:true,dataIndex:'JobStart',align:'left'},
    {header:'Job End',sortable:true,dataIndex:'JobEnd',align:'left'},
    {header:'Worker Node',sortable:true,dataIndex:'WorkerNode',align:'left',hidden:true},
    {header:'File Type',sortable:true,dataIndex:'FileType',align:'left',hidden:true},
    {header:'Event Type Id',sortable:true,dataIndex:'EvtTypeId',align:'left',hidden:true}
  ];
  var store = initStore(record);
  dataMngr['store'] = store;
  var title = '';
  var topbarBKButton = {
    cls:"x-btn-icon",
    handler:function(){
      var text = inputBKPath.getRawValue();
      addBookmark(text);
    },
    icon:gURLRoot+'/images/iface/advanced.gif',
    minWidth:'20',
    tooltip:'Add the path in the left text field to your bookmarks',
  }
  var inputBKPath = new Ext.form.TextField({
    allowBlank:false,
    enableKeyEvents:true,
    fieldLabel:'BK Path',
    id:'BKPath'
  });
  inputBKPath.on('keypress',function(object,e){
    var keyCode = e.getKey()
    if(keyCode == e.ENTER){
      var bkTreePanel = Ext.getCmp('bkTreePanel');
      var bkRawPath = inputBKPath.getRawValue();
      expandBookmarkPath(bkRawPath);
    }
  });
  var goButton = {
    cls:"x-btn-text-icon",
    handler:function(){
      var bkTreePanel = Ext.getCmp('bkTreePanel');
      var bkRawPath = inputBKPath.getRawValue();
      expandBookmarkPath(bkRawPath);
    },
    icon:gURLRoot+'/images/iface/go.gif',
    tooltip:'Loading the demanded location could take time',
    text:'Go',
    minWidth:50
  }
  var tbar = [topbarBKButton,inputBKPath,goButton];
  tableMngr = {'store':store,'columns':columns,'title':title,'tbar':tbar,'bk':true};
  var dataTable = table(tableMngr);
  dataTable.addListener('cellclick',function(table,rowIndex,columnIndex){
    showMenu('main',table,rowIndex,columnIndex);
  });
  dataTable.addListener('resize',function(){
    var tmpWidth = dataTable.getInnerWidth() - 6 - 50 - 20 - 2;
    inputBKPath.setWidth(tmpWidth);
  });
  return dataTable
}
function initBookmarkPanel(){
  function deleteBox(flag){
    var img = '<img src="'+gURLRoot+'/images/iface/close.gif">';
    return img
  }
  var store = new Ext.data.SimpleStore({ // Quick truck to trigger the initial load function
    autoLoad:true,
    baseParams:{'ping':true},
    fields:['Name','Value'], // Unfortunately the reader is array reader not JSON
    url:'action'
  });
  store.on('load',storeFunction = function(){
    Ext.Ajax.request({
      failure:function(response){
        store.loadData({});
      },
      method:'POST',
      params:{'getBookmarks':true},
      success:function(response){
        var jsonData = Ext.util.JSON.decode(response.responseText);
        if(jsonData['success'] == 'false'){
          store.loadData({});
        }else{
          var serverResult = jsonData['result'];
          var result = []
          for(i in serverResult){
            result.push([i,serverResult[i]]);
          }
          store.loadData(result);
        }
      },
      url:'action'
    });
    store.removeListener('load',storeFunction);
  });
//  Data sample: [[0,'Test Record','/Ot']];
  function syncButton(minWidth){
    if(!minWidth){
      minWidth = '';
    }
    var button = new Ext.Button({
      cls:"x-btn-text-icon",
      handler:function(){
        bookmarkSync('get');
      },
      icon:gURLRoot+'/images/iface/refresh.gif',
      minWidth:minWidth,
      text:'Load Bookmarks',
      tooltip:'Download the bookmarks from the service. This action can resolve the conflicts between Bookkeeping browser instances',
    });
    return button
  }
  var table = new Ext.grid.GridPanel({
    autoScroll:true,
    bbar:new Ext.Toolbar({
      items:[syncButton()]
    }),
    border:false,
    columns:[
      {header:'Name',sortable:true,dataIndex:'Name',align:'left',hideable:false,css:'cursor:pointer;cursor:hand;'},
      {header:'',width:26,sortable:false,renderer:deleteBox,hideable:false,fixed:true,menuDisabled:true,css:'cursor:pointer;cursor:hand;'}//background-color:#F2D4CD;}
    ],
    collapsible:true,
    id:'bookmarkPanel',
    labelAlign:'top',
    loadMask:true,
    minWidth:'200',
    store:store,
    stripeRows:true,
    title:'Bookmarks',
    viewConfig:{forceFit:true,scrollOffset:1}
  });
  table.addListener('cellclick',function(table,rowIndex,columnIndex){
    var record = table.getStore().getAt(rowIndex); // Get the Record for the row
    if(columnIndex == 1){
      var title = record.get('Name');
      var path = record.get('Value');
      Ext.Msg.show({
        title:'Delete bookmark?',
        msg:'This action will delete the bookmark "<b>' + title + '</b>" localy and from remote repository. Are you really want to delete it?',
        buttons:Ext.Msg.YESNO,
        fn:function(btn){
          if(btn == 'yes'){
            bookmarkSync('del',path,title) // TODO syncronisation here
          }
        },
        icon:Ext.MessageBox.QUESTION
      });
    }else{
      var value = record.get('Value');
      try{
        var tree = Ext.getCmp('bkTreePanel');
      }catch(e){
        alert('Error: Unable to get Bookkeeping tree component');
        return
      }
      table.collapse(false);
      tree.expand(false);
      expandBookmarkPath(value);
    }
    var tt = 0;
  });
  table.addListener('resize',function(){
    var tmpWidth = table.getInnerWidth() - 4;
    var button = syncButton(tmpWidth);
    var bar = table.getBottomToolbar();
    Ext.fly(bar.items.get(0).getEl()).remove();
    bar.items.removeAt(0);
    bar.insertButton(0,button);
    table.doLayout();
  });
  return table
}




function afterDataLoad(){
  var table = Ext.getCmp('DataMonitoringTable');
  if(table){
    try{
      var nof = Ext.getCmp('nof');
      nof.setRawValue('');
      if(table.store.totalLength){
        nof.setRawValue(table.store.totalLength);
//        delete table.store.totalLength; 
      }
    }catch(e){}
    try{
      var noe = Ext.getCmp('noe');
      noe.setRawValue('');
      if(table.store.extra_msg.GlobalStatistics.NumberofEvents){
        noe.setRawValue(table.store.extra_msg.GlobalStatistics.NumberofEvents);
        delete table.store.extra_msg.GlobalStatistics.NumberofEvents;
      }
    }catch(e){}
    try{
      var fSize = Ext.getCmp('fSize');
      fSize.setRawValue('');
      if(table.store.extra_msg.GlobalStatistics.FilesSize){
        fSize.setRawValue(table.store.extra_msg.GlobalStatistics.FilesSize);
        delete table.store.extra_msg.GlobalStatistics.FilesSize;
      }
    }catch(e){}
    try{
      var pass = Ext.getCmp('procPass');
      pass.setRawValue('');
      if(table.store.extra_msg.Selection.ProcessingPass){
        pass.setRawValue(table.store.extra_msg.Selection.ProcessingPass);
        delete table.store.extra_msg.Selection.ProcessingPass;
      }
    }catch(e){}
    try{
      var eType = Ext.getCmp('eType');
      eType.setRawValue('');
      if(table.store.extra_msg.Selection.Eventtype){
        eType.setRawValue(table.store.extra_msg.Selection.Eventtype);
        delete table.store.extra_msg.Selection.Eventtype;
      }
    }catch(e){}
    try{
      var cName = Ext.getCmp('cName');
      cName.setRawValue('');
      if(table.store.extra_msg.Selection.ConfigurationName){
        cName.setRawValue(table.store.extra_msg.Selection.ConfigurationName);
        delete table.store.extra_msg.Selection.ConfigurationName;
      }
    }catch(e){}
    try{
      var cVersion = Ext.getCmp('cVersion');
      cVersion.setRawValue('');
      if(table.store.extra_msg.Selection.ConfigurationVersion){
        cVersion.setRawValue(table.store.extra_msg.Selection.ConfigurationVersion);
        delete table.store.extra_msg.Selection.ConfigurationVersion;
      }
    }catch(e){}
    try{
      var production = Ext.getCmp('production');
      production.setRawValue('');
      if(table.store.extra_msg.Selection.Production){
        production.setRawValue(table.store.extra_msg.Selection.Production);
        delete table.store.extra_msg.Selection.Production;
      }
    }catch(e){}
    try{
      var simCond = Ext.getCmp('simCond');
      simCond.setRawValue('');
      if(table.store.extra_msg.Selection.SimulationCondition){
        simCond.setRawValue(table.store.extra_msg.Selection.SimulationCondition);
        delete table.store.extra_msg.Selection.SimulationCondition;
      }
    }catch(e){}
    try{
      var fType = Ext.getCmp('fType');
      fType.setRawValue('');
      if(table.store.extra_msg.Selection.FileType){
        fType.setRawValue(table.store.extra_msg.Selection.FileType);
        delete table.store.extra_msg.Selection.FileType;
      }
    }catch(e){}
    try{
      var nameVersion = Ext.getCmp('nameVersion');
      nameVersion.setRawValue('');
      var ver = '';
      var name = '';
      if(table.store.extra_msg.Selection.Programname){
        name = table.store.extra_msg.Selection.Programname;
        delete table.store.extra_msg.Selection.Programname;
      }
      if(table.store.extra_msg.Selection.Programversion){
        ver = table.store.extra_msg.Selection.Programversion;
        delete table.store.extra_msg.Selection.Programversion;
      }
      nameVersion.setRawValue(name + ' ' + ver);
    }catch(e){}
  }
  var rBar = Ext.getCmp('bkRight');
  try{
    if(rBar){
      rBar.enable();
    }
  }catch(e){}
}
function AJAXsuccess(value,id,response){
  gMainLayout.container.unmask();
  var jsonData = Ext.util.JSON.decode(response);
  if(jsonData['success'] == 'false'){
    alert('Error: ' + jsonData['error']);
    return
  }
  var result = jsonData['result'];
  var panel = {};
  if(value == 'getLogInfoLFN'){
    var reader = {};
    var columns = [];
    reader = new Ext.data.ArrayReader({},[
      {name:'Status'},
      {name:'MinorStatus'},
      {name:'StatusTime'},
      {name:'Source'}
    ]);
    columns = [
      {header:'Status',sortable:true,dataIndex:'Status',align:'left'},
      {header:'MinorStatus',sortable:true,dataIndex:'MinorStatus',align:'left'},
      {header:'StatusTime [UTC]',sortable:true,dataIndex:'StatusTime',align:'left'},
      {header:'Source',sortable:true,dataIndex:'Source',align:'left'}
    ];
    var store = new Ext.data.Store({
      data:result,
      reader:reader
    });
    panel = new Ext.grid.GridPanel({
      anchor:'100%',
      columns:columns,
      store:store,
      stripeRows:true,
      viewConfig:{forceFit:true}
    });
    panel.addListener('cellclick',function(table,rowIndex,columnIndex){
      showMenu('nonMain',table,rowIndex,columnIndex);
    });
  }
  id = setTitle(value,id);
  displayWin(panel,id);
}
