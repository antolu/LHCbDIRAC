// Initialisation of selection sidebar, all changes with selection items should goes here
function initTree(){
  var root = new Ext.tree.AsyncTreeNode({
     draggable:false,
     expanded:true,
     extra:'/',
     id:'BKSource',
     text: '/'
  });
  var loader = new Ext.tree.TreeLoader({
    dataUrl:'submit',
    id:'BKLoader',
    requestMethod:'POST'
  });
  loader.on('beforeload', function(treeLoader, node){
    try{
      this.baseParams.root = node.attributes.extra;
      this.baseParams.level = node.attributes.qtip;
    }catch(e){}
  });
  loader.on('load',function(tree,node,response){
    try{
      var jsonData = Ext.util.JSON.decode(response.responseText);
      if(jsonData['success'] == 'false'){
        alert('Error: ' + jsonData['error']);
        return
      }
    }catch(e){
      alert('Error: ' + e.name + ': ' + e.message);
      return
    }
  });
  var tree = sideTree(loader,'Bookkeeping browser',root);
  tree.addListener('expandnode',function(node){
    insertPath(node);
  });
  tree.addListener('collapsenode',function(node){
    insertPath(node);
  });
  tree.addListener('click',function(node){
    if(node.leaf){
      insertPath(node);
      loadLeaf(node);
    }
  });
  return tree
}



function setMenuItems(selections){
  if(selections){
    var lfn = selections.Name;
  }else{
    return
  }
  if(dirac.menu){
    dirac.menu.add(
      {handler:function(){AJAXrequest('getLogInfoLFN',lfn)},text:'Logging Information'}
    );
  }
};



function expandBookmarkPath(origPath){
  var tree = Ext.getCmp('bkTreePanel');
  try{
    var root = tree.getRootNode();
    if(root.childNodes){
      var childrens = root.childNodes;
      var nodesLength = childrens.length;
    }else{
      alert('Error: This tree does not have any nodes');
      return
    }
    var loader = tree.getLoader();
    var bar = tree.getBottomToolbar();
    var advBtn = bar.items.get(2);
  }catch(e){
    alert('Error: ' + e.name + ': ' + e.message);
    return
  }
  var split = origPath.split(':/');
  var prefix = split[0];
  var path = split[1];
  path = path.split('/');
  var length = path.length;
  for(i=0; i < length; i++){
    if(path[i].length == 0){
      path.splice(i,1);
      length = path.length;
      i = i - 1;
    }
  }
  if(length < 1){
    alert('Error: Bookmark path is too short to expand: ' + path);
    return
  }
  var arg = 'bkSim'; // Default
  prefix = prefix.split('+');
  if(prefix[0] == 'evt'){
    arg = 'bkEvent';
  }else if(prefix[0] == 'prod'){
    arg = 'bkProd';
  }else if(prefix[0] == 'run'){
    arg = 'bkRun';
  }
  var adv = false; // Need to reload root to show advanced option? (true|false) true to reload
  if(prefix[1] == 'adv'){
    if(!advBtn.trigger){
      adv = true;
      loader.baseParams.adv = true;
      var newBtn = advButton('chk');
      Ext.fly(advBtn.getEl()).remove();
      bar.items.removeAt(1);
      bar.insertButton(2,newBtn);
      tree.doLayout();
    }
  }else{
    if(advBtn.trigger){
      adv = true;
      loader.baseParams.adv = false;
      var newBtn = advButton('unchk');
      Ext.fly(advBtn.getEl()).remove();
      bar.items.removeAt(1);
      bar.insertButton(2,newBtn);
      tree.doLayout();
    }
  }
  if((arg = 'bkSim')||(arg = 'bkEvent')){
    if(bkAction(arg)){
      loader.addListener('load',loadFunction = function(newNode){
        expRec(root,path);
        loader.removeListener('load',loadFunction);
      });
      root.reload();
    }else{
      if(adv){
        loader.addListener('load',loadFunction = function(newNode){
          expRec(root,path);
          loader.removeListener('load',loadFunction);
        });
        root.reload();
      }else{
        expRec(root,path);
      }
    }
  }else{

  }
  return
}
function expRec(tmpNode,path){
  try{
    tmpNode.removeListener('expand',tmpFunction);
  }catch(e){}
  var first = path.shift();
  if(first.length > 0){
    var node = tmpNode.findChild('text',first);
    if(node){
      node.select();
      if(node.leaf){
        loadLeaf(node);
        return
      }
      if(!node.isLoaded()){
        node.addListener('expand',tmpFunction = function(newNode){
          expRec(newNode,path);
        });
        node.expand();
      }else if(node.isExpanded()){
        expRec(node,path);
      }else{
        node.expand();
        expRec(node,path);
      }
      if(path.length == 0){
        insertPath(node);
      }
    }else{
      alert('Error: Can not find node: ' + first);
      return
    }
  }
}
function bkAction(value){
  var tree = Ext.getCmp('bkTreePanel');
  var loader = tree.getLoader();
  var root = tree.getRootNode();
  var bottomToolbar = tree.getBottomToolbar();
  try{
    var menuButton = bottomToolbar.items.items[0];
    loader.baseParams;
  }catch(e){
    alert('Error: ' + e.name + ': ' + e.message);
    return
  }
  var oldText = menuButton.getText();
  var text = 'SimConditions first';
  if(value == 'bkFile'){
    text = 'File Lookup';
  }else if(value == 'bkRun'){
    text = 'Run Lookup';
  }else if(value == 'bkProd'){
    text = 'Production Lookup';
  }else if(value == 'bkSim'){
    text = 'SimConditions first';
  }else if(value == 'bkEvent'){
    text = 'Event type first';
  }
  if((oldText == text)&&((value == 'bkSim')||(value == 'bkEvent'))){
    return false
  }
  menuButton.setText(text);
  var advButton = bottomToolbar.items.get(2);
  if((value == 'bkRun')||(value == 'bkProd')){
    advButton.disable();
    if(value == 'bkRun'){
      var title = 'Run Lookup';
      var label = 'Please enter the run number:';
    }else{
      var title = 'Production Lookup';
      var label = 'Please enter the production number:';
    }
    Ext.Msg.prompt(title,label,function(btn,text){
      if(btn == 'ok'){
        if(text){
          loader.baseParams.mode = value;
          if(value == 'bkRun'){
            root.attributes.extra = '/RUN_' + text;
          }else{
            root.attributes.extra = '/PROD_' + text;
          }
          text = '/' + text;
          root.setText(text);
          root.reload();
          return true
        }else{
          advButton.enable();
          menuButton.setText(oldText);
          this.hide();
          return false
        }
      }else{
        advButton.enable();
        menuButton.setText(oldText);
        return false
      }
    });
  }else if(value == 'bkFile'){
    var title = 'File Lookup';
    var label = 'Please enter the LFN:';
    Ext.Msg.prompt(title,label,function(btn,text){
      if(btn == 'ok'){
        if(text){
          loader.baseParams.mode = value;
          loader.baseParams.value = text;
          return true
        }else{
          menuButton.setText(oldText);
          this.hide();
          return false
        }
      }else{
        menuButton.setText(oldText);
        return false
      }
    });
  }else if((value == 'bkSim')||(value == 'bkEvent')){
    advButton.enable();
    root.attributes.extra = '/';
    root.setText('/');
    loader.baseParams.mode = value;
  }
  return true
}
function advButton(state){
  var icon = gURLRoot+'/images/iface/unchecked.gif';
  var trigger = false;
  if(state == 'chk'){
    icon = gURLRoot+'/images/iface/checked.gif';
    trigger = true;
  }else{
    icon = gURLRoot+'/images/iface/unchecked.gif';
    trigger = false;
  }
  var button = new Ext.Button({
    cls:"x-btn-text-icon",
    handler:function(){
      var tree = Ext.getCmp('bkTreePanel');
      var bar = tree.getBottomToolbar();
      var loader = tree.getLoader();
      var root = tree.getRootNode();
      try{
        if(loader.baseParams.adv){
          loader.baseParams.adv = false;
          var newBtn = advButton('unchk');
        }else{
          loader.baseParams.adv = true;
          var newBtn = advButton('chk');
        }
      }catch(e){
        loader.baseParams.adv = false;
        var newBtn = advButton('unchk');
      }
      Ext.fly(bar.items.get(2).getEl()).remove();
      bar.items.removeAt(1);
      bar.insertButton(2,newBtn);
      tree.doLayout();
      root.reload();
    },
    icon:icon,
    text:'Advanced',
    trigger:trigger,
    tooltip:'Show more details if checked'
  });
  return button
}
function sideTree(loader,title,root){
  var menuButton =  new Ext.Toolbar.Button({
    text:'SimConditions first',
    menu:[
      {text:'SimConditions first',handler:function(){if(bkAction('bkSim')){root.reload();}}},
      {text:'Event type first',handler:function(){if(bkAction('bkEvent')){root.reload();}}},
      {text:'File Lookup',handler:function(){if(bkAction('bkFile')){root.reload();}}},
      {text:'Production Lookup',handler:function(){bkAction('bkProd')}},
      {text:'Run Lookup',handler:function(){bkAction('bkRun')}}
    ]
  });
  var advancedButton = advButton('unchk');
  var bkButton = new Ext.Button({
    cls:"x-btn-text-icon",
    handler:function(){
      addBookmark();
    },
    icon:gURLRoot+'/images/iface/advanced.gif',
    text:'Add Bookmark',
    tooltip:'You can add bookmark using drag-n-drop on the window'
  });
  var refreshButton = new Ext.Button({
    cls:"x-btn-text-icon",
    handler:function(){
      root.reload();
    },
    icon:gURLRoot+'/images/iface/refresh.gif',
    text:'Refresh',
    tooltip:'All expanded branches will be collapsed'
  });
  var tree = new Ext.tree.TreePanel({
    animate:true,
    autoScroll:true,
    bbar:new Ext.Toolbar({
      items:[menuButton,'->',advancedButton,bkButton,refreshButton]
    }),
    border:false,
    buttonAlign:'center',
    collapsible:true,
    columns:[{
      header:'Tree',
      width:300,
      dataIndex:'text'
    },{
      header:'Description',
      width:90,
      dataIndex:'qtip'
    }],
    containerScroll:true,
    cmargins:'2 2 2 2',
    ddGroup:'bkDDGroup',
    enableDrag:true,
    enableDrop:false,
    id:'bkTreePanel',
    labelAlign:'top',
    lines:true,
    loader:loader,
    margins:'2 0 2 0',
    minWidth:400,
    region:'west',
    root:root,
    split:true,
    title:title,
    width:400
  });
  var contextMenu = new Ext.menu.Menu();
  tree.addListener('contextmenu',function(node,event){
    var text = fullPath(node);
    contextMenu.removeAll();
    contextMenu.add({
        handler:function(){
          addBookmark(text);
        },
        icon:gURLRoot + '/images/iface/advanced.gif',
        text:'Add Bookmark'
      },{
        handler:function(){
          Ext.Msg.alert('Show BK path',text);
        },
        icon:gURLRoot + '/images/iface/showPath.gif',
        text:'Show BK path'
// TODO show more information, suplied by service
//      },{
//        handler:function(){
//          Ext.Msg.alert('More Information',text);
//        },
//        icon:gURLRoot + '/images/iface/info.gif',
//        text:'More Information'
      });
    contextMenu.show(node.ui.getAnchor());
  });
  return tree
}
function addBookmark(path){
  try{
    var tmpPanel = Ext.getCmp('addBookmarkPanel');
    if(tmpPanel){
      Ext.Msg.alert('Warning', 'Only one bookmark dialog can be displayed');
      return
    }
    delete tmpPanel
  }catch(e){}
  var panel = new Ext.FormPanel({
    bodyStyle:'padding: 5px',
    buttonAlign:'center',
    buttons:[{
      cls:"x-btn-text-icon",
      handler:function(){
        transferBookmark()
      },
      icon:gURLRoot+'/images/iface/advanced.gif',
      minWidth:'150',
      tooltip:'Add the link in the input field to the bookmark panel',
      text:'Add bookmark'
    },{
      cls:"x-btn-text-icon",
      handler:function(){
        var parent = panel.findParentByType('window');
        parent.close();
      },
      icon:gURLRoot+'/images/iface/reset.gif',
      minWidth:'100',
      tooltip:'Click here to discard changes and close the window',
      text:'Cancel'
    }],
    id:'addBookmarkPanel',
    items:[{
      allowBlank:false,
      anchor:'100%',
      allowBlank:true,
      enableKeyEvents:true,
      id:'titleField',
      fieldLabel:'Title',
      selectOnFocus:true,
      xtype:'textfield'
    },{
      allowBlank:false,
      anchor:'100%',
      allowBlank:true,
      id:'aField',
      enableKeyEvents:true,
      fieldLabel:'Address',
      selectOnFocus:true,
      xtype:'textfield'
    },{
      anchor:'100%',
      fieldLabel:'Tip',
      html:'You can create a bookmark draging a branch or a node from the BK tree and droping it over this window',
      xtype:'label'
    }],
    labelAlign:'top'
  });
  var window = displayWin(panel,'Create bookmark','',true,false);
  window.setSize(400,210);
  var field = Ext.getCmp('aField');
  var title = Ext.getCmp('titleField');
  if((path != null) || (path != '')){
    field.setValue(path);
    title.setValue(path);
  }
  var dropTargetEl = window.body.dom;
  var dropTarget = new Ext.dd.DropTarget(dropTargetEl,{
    ddGroup:'bkDDGroup',
    notifyEnter:function(ddSource, e, data){
      panel.body.stopFx();
      panel.body.highlight();
    },
    notifyDrop:function(ddSource, e, data){
      try{
        var path = data.node.getPath();
        var node = data.node;
      }catch(e){
        alert('Error: Unable to get node path');
        return 
      }
      var bkText = bkPath(data.node);
      field.setValue(bkText);
      title.setValue(bkText);
      return(true);
    }
  });
}
function transferBookmark(){
  try{
    var bkPanel = Ext.getCmp('bookmarkPanel');
    if(!bkPanel){
      alert('Error: Bookmark panel does not exist');
      return
    }
  }catch(e){
    alert('Error: Can not get the bookmark panel');
    return
  }
  try{
    var field = Ext.getCmp('aField').getRawValue();
    var title = Ext.getCmp('titleField').getRawValue();
  }catch(e){
    alert('Error: Can not get one or both of these components: aField, titleField');
    return
  }
  if(title.length <= 0){
    alert('Error: Please enter the bookmark description ');
    return
  }
  if(field.length <= 0){
    alert('Error: Unable to create bookmark with empty address');
    return
  }
  if((field) && (title)){
    var panel = Ext.getCmp('addBookmarkPanel');
    var parent = panel.findParentByType('window');
    parent.close();
    if(!uniqueBookmark(title)){
      return false
    }
    var record = new bkPanel.store.recordType({'Flag':0,'Name':title,'Value':field});
    bkPanel.store.add(record);
    bookmarkSync('add',field,title); // TODO Syncronisation here
  }else{
    alert('Error: The path does not exist');
    return
  }
}
function bkPath(node){
  try{
    var depth = node.getDepth();
  }catch(e){
    alert('Error: Unable to get tree depth');
    return
  }
  if(depth >= 0){
    var tmpNode = node;
    var bkText = '';
    for(var i = 0; i < depth; i++){
      var newTxt = tmpNode.parentNode.text;
      if(newTxt == '/'){
        bkText = '/' + bkText;
      }else{
        bkText = newTxt + '/' + bkText;
      }
      tmpNode = tmpNode.parentNode
    }
    bkText = bkText + node.text;
  }else{
    alert('Error: Tree depth is negative');
    return
  }
  return bkText
}
function createField(id,label){
  var txtField = new Ext.form.TextField({
    anchor:'90%',
    fieldLabel:label,
    id:id,
    name:id,
    readOnly:true
  });
  return txtField;
}
function saveFile(panel){
  Ext.util.FileOps = function(obj, name){
    alert('This code was posted in \[CODE\] tags');
  };
  Ext.util.FileOps.downloadFile = function(url) {
    var id = Ext.id();
    var frame = document.createElement('iframe');
    frame.id = id;
    frame.name = id;
    frame.className = 'x-hidden';
    if(Ext.isIE) {
      frame.src = Ext.SSL_SECURE_URL;
    }
    document.body.appendChild(frame);
    if(Ext.isIE) {
      document.frames[id].name = id;
    }
    var form = Ext.DomHelper.append(document.body, {
      tag:'form',
      method:'post',
      action:url,
      target:id
    });
    document.body.appendChild(form);
    var callback = function() {
      Ext.EventManager.removeListener(frame, 'load', callback, this);
      setTimeout(function() {document.body.removeChild(form);}, 100);
      setTimeout(function() {document.body.removeChild(frame);}, 110);
      gMainLayout.container.unmask();
    };
    Ext.EventManager.on(frame, 'load', callback, this);
    form.submit();
  };
  try{
    var table = Ext.getCmp('DataMonitoringTable');
    if(table.store.baseParams.root){
      var root = table.store.baseParams.root;
    }else{
      alert('Error: There are no records in the table');
      return
    }
  }catch(e){
    alert('Error: There are no records in the table');
    return
  }
  var params = 'root=\'' + root + '\'';
  params = params.replace(/ \+ /gi,"*spaceplusspace*")
  try{
    var type = 'txt';
    for(i=0;i<document.filetypeHTML.radiobutton.length;i++) {
      if(document.filetypeHTML.radiobutton[i].checked) {
        type = document.filetypeHTML.radiobutton[i].value;
      }
    }
    var records = 'all';
    for(i=0;i<document.recordsHTML.radio.length;i++) {
      if(document.recordsHTML.radio[i].checked) {
        records = document.recordsHTML.radio[i].value;
      }
    }
    var start = 0;
    var end = table.store.totalLength;
    if(records == 'recs'){
      start = document.recordsHTML.textfield1.value;
      if(start == ''){
        alert('Initial record index is absent');
        return
      }
      end = document.recordsHTML.textfield2.value;
      if(end == ''){
        alert('Final record index is absent');
        return
      }
    }
    var fname =document.saveHTML.savefield.value;
  }catch(e){
    alert('Error: Can not get values from the form');
    return
  }
  params = params + '&type=' + type + '&start=' + start + '&limit=' + end + '&fname=' + fname;
  saveMask();
  Ext.util.FileOps.downloadFile('download?' + params);
}
function saveMask(){
  var msg = '<table><tr><td><h1 style="font-size:16px;white-space:nowrap;">';
  msg = msg + 'Server response may take time. Please be patient';
  msg = msg + '</h1></td></tr><tr><td><p style="font-size:12px">';
  msg = msg + 'This window will close automatically in <span id="closeCount" style="color:#009900;font-weight:bold;">10</span> seconds';
  msg = msg + '</p></td></tr></table>';
  var window = Ext.Msg.show({
    animEl: 'elId',
    buttons: Ext.Msg.OK,
    icon: Ext.MessageBox.INFO,
    minWidth:300,
    msg:msg,
    title:'Please, wait'
  });
  var runner = new Ext.util.TaskRunner();
  var task = {
    run:countdown,
    interval:1000 //1 second
  }
  var c = 10;
  function countdown(){
    c = c - 1;
    if(c <= 0){
      runner.stop(task);
      window.hide();
    }else{
      document.getElementById('closeCount').innerHTML = c;
    }
  }
  runner.start(task);
}
function bkSaveDialog(){
  try{
    var table = Ext.getCmp('DataMonitoringTable');
    if(!table.store.totalLength){
      alert('Error: There are no records in the table');
      return
    }
  }catch(e){
    alert('Error: There are no records in the table');
    return
  }
  var name = 'BK_default_name';
  try{
    name = table.store.extra_msg.SaveAs;
  }catch(e){}
  var filetypeHTML = '<form id="filetypeHTML" name="filetypeHTML" method="post" action="">';
  filetypeHTML = filetypeHTML + '<table width="350" border="0" cellspacing="5" cellpadding="0">';
  filetypeHTML = filetypeHTML + '<tr><td width="20"><label valign="middle">';
  filetypeHTML = filetypeHTML + '<input name="radiobutton" type="radio" value="txt" tabindex="1" checked="checked"/></label>';
  filetypeHTML = filetypeHTML + '</td><td>Save as a text file (*.txt) </td>';
  filetypeHTML = filetypeHTML + '</tr><tr><td><label>';
  filetypeHTML = filetypeHTML + '<input name="radiobutton" type="radio" value="py" tabindex="2" /></label>';
  filetypeHTML = filetypeHTML + '</td><td>Save as a python file (*.py)</td></tr></table></form>';
  var recordsHTML = '<form id="recordsHTML" name="recordsHTML" method="post" action="">';
  recordsHTML = recordsHTML + '<table width="350" border="0" cellspacing="5" cellpadding="0"><tr><td width="20"><label>';
  recordsHTML = recordsHTML + '<input name="radio" id="radioAll" type="radio" tabindex="3" value="all" checked="checked" onChange="rClick(\'All\')" /></label>';
  recordsHTML = recordsHTML + '</td><td colspan="5">All</td></tr><tr><td><label>';
  recordsHTML = recordsHTML + '<input name="radio" id="radioR" type="radio" tabindex="4" value="recs" onChange="rClick(\'radioR\')" /></label>';
  recordsHTML = recordsHTML + '</td><td>Records</td><td align="right">From:</td><td><label>';
  recordsHTML = recordsHTML + '<input id="textF1" name="textfield1" type="text" size="10" disabled="disabled" /></label>';
  recordsHTML = recordsHTML + '</td><td align="right">To:</td><td><label>';
  recordsHTML = recordsHTML + '<input id="textF2" name="textfield2" type="text" size="10" disabled="disabled" /></label>';
  recordsHTML = recordsHTML + '</td></tr></table></form>';
  var saveHTML = '<form id="saveHTML" name="saveHTML" method="post" action="">';
  var saveHTML = saveHTML + '<table width="350" border="0" cellspacing="5" cellpadding="0"><tr><td>';
  var saveHTML = saveHTML + '<label><input id="saveF" name="savefield" type="text" size="50" value="' + name + '" /></label>';
  var saveHTML = saveHTML + '</td></tr></table></form>';
  var panel = new Ext.Panel({
    labelAlign: 'top',
    bodyStyle:'padding:5px',
    buttonAlign:'center',
    buttons:[{
      cls:"x-btn-text-icon",
      handler:function(){
        saveFile(panel);
        var parent = panel.findParentByType('window');
        parent.close();
      },
      icon:gURLRoot+'/images/iface/save.gif',
      minWidth:'150',
      tooltip:'Will open a standard browser save file dialog',
      text:'Save'
    },{
      cls:"x-btn-text-icon",
      handler:function(){
        var parent = panel.findParentByType('window');
        parent.close();
      },
      icon:gURLRoot+'/images/iface/reset.gif',
      minWidth:'100',
      tooltip:'Click here to discard changes and close the window',
      text:'Cancel'
    },{
      cls:"x-btn-text-icon",
      handler:function(){
        getBKInfo('textF1','textF2');
      },
      icon:gURLRoot+'/images/iface/info.gif',
      id:'infoButtonRec',
      minWidth:'100',
      text:'Info',
      tooltip:'Get statistical information for the chosen entries'
    }],
    items:[{
      autoHeight:true,
      html:filetypeHTML,
      title:'File Type',
      xtype:'fieldset'    
    },{
      autoHeight:true,
      html:recordsHTML,
      title:'Records',
      xtype:'fieldset'
    },{
      autoHeight:true,
      html:saveHTML,
      title:'Save as',
      xtype:'fieldset'
    }]
  });
  var window = displayWin(panel,'Save dialog','true',true);
//  window.setSize(400,265);
  window.setSize(400,330);
}
function getBKInfo(first,last){
  var table = Ext.getCmp('DataMonitoringTable');
  var root = '';
  try{
    if(table.store.baseParams.level){
      if(table.store.baseParams.level == 'showFiles'){
        root = table.store.baseParams.root;
      }else{
        alert('Unable to get table.store.baseParams.root value');
        return;
      }
    }
  }catch(e){
    alert('Unable to get table.store.baseParams.level value');
    return;
  }
  var len = 0;
  try{
    if(table.store.totalLength){
      len = table.store.totalLength;
    }
  }catch(e){
    alert('Unable to get table.store.totalLength value');
    return
  }
  var t1 = document.getElementById(first);
  var t2 = document.getElementById(last);
  var all = document.getElementById('radioAll');
  if(all.checked){
    t1 = 0;
    t2 = len;
  }else{
    if(t1.value == ''){
      alert('Field "From" is empty');
      return
    }else if(t1.value > len){
      alert('The value in the field "From" is bigger than the total number of entries');
      return
    }else{
      t1 = t1.value;
    }
    if(t2.value == ''){
      alert('Field "To" is empty');
      return
    }else if(t2.value > len){
      alert('The value in the field "To" is bigger than the total number of entries');
      return
    }else{
      t2 = t2.value;
    }
  }
  var url ='info?&root=' + root + '&start=' + t1 + '&limit=' + t2;
  var panel = new Ext.Panel({autoLoad:url,bodyStyle:'padding: 5px'});
  var window = displayWin(panel,'Statistics','',true);
  window.setSize(200,100);
  window.center();
}
function rClick(radioID){
  var t1 = document.getElementById('textF1');
  var t2 = document.getElementById('textF2');
  if(radioID == 'radioR'){
    t1.disabled = false;
    t2.disabled = false;
  }else if(radioID == 'radioP'){
    t1.disabled = true;
    t2.disabled = true;
    t1.value = '';
    t2.value = '';
  }else{
    t1.disabled = true;
    t2.disabled = true;
    t1.value = '';
    t2.value = '';
  }
}
function initRunLookup(){
  function submitForm(){
    var bar = Ext.getCmp('sideBar');
    bar.setActiveItem(0);
    if(table){
      try{
        table.store.proxy.conn.url = 'action';
        var formValues = panel.form.getValues();
        table.store.baseParams = formValues;
        table.store.baseParams.byRun = 'true';
        table.store.baseParams.limit = table.bottomToolbar.pageSize;
        table.store.load();
      }catch(e){
        alert('Error: ' + e.name + ': ' + e.message);
        return
      }
    }

  }
  var reset = new Ext.Button({
    cls:"x-btn-text-icon",
    handler:function(){
      panel.form.reset();
    },
    icon:gURLRoot+'/images/iface/reset.gif',
    minWidth:'70',
    tooltip:'Reset values in the form',
    text:'Reset'
  });
  var submit = new Ext.Button({
    cls:"x-btn-text-icon",
    handler:function(){
      submitForm();
    },
    icon:gURLRoot+'/images/iface/submit.gif',
    minWidth:'70',
    tooltip:'Send request to the server',
    text:'Submit'
  });
  var prod = new Ext.form.NumberField({
    anchor:'90%',
    allowBlank:false,
    allowDecimals:false,
    allowNegative:false,
    fieldLabel:'Run ID',
    mode:'local',
    name:'runID',
    selectOnFocus:true,
    value:0
  })
  var panel = new Ext.FormPanel({
    autoScroll:true,
    bodyStyle:'padding: 5px',
    border:false,
    buttonAlign:'center',
    buttons:[submit,reset],
    collapsible:true,
    id:'initRunLookup',
    items:[prod],
    labelAlign:'top',
    method:'POST',
    minWidth:'200',
    title:'Run Lookup',
    url:'submit',
    waitMsgTarget:true
  });
  return panel
}
function fullPath(node){
  var path = bkPath(node);
  var tree = Ext.getCmp('bkTreePanel');
  var loader = tree.getLoader();
  var prefix = 'sim';
  try{
    if(loader.baseParams){
      if(loader.baseParams['mode'] == 'bkEvent'){
        prefix = 'evt';
      }else if(loader.baseParams['mode'] == 'bkProd'){
        prefix = 'prod';
      }else if(loader.baseParams['mode'] == 'bkRun'){
        prefix = 'run';
      }
      if(loader.baseParams.adv){
        prefix = prefix + '+adv';
      }
    }
  }catch(e){
    alert('Error: ' + e.name + ': ' + e.message);
    return
  }
  path = prefix + ':/' + path;
  return path
}
function insertPath(node){
  var path = fullPath(node);
  var txtField = Ext.getCmp('BKPath');
  txtField.setValue(path);
}
function loadLeaf(node){
  insertPath(node);
  node.ui.removeClass("x-tree-node-leaf");
  node.ui.removeClass("x-tree-node-collapsed");
  node.ui.addClass("x-tree-node-expanded");
  var table = Ext.getCmp('DataMonitoringTable');
  if(table){
    try{
      var tree = Ext.getCmp('bkTreePanel');
      var loader = tree.getLoader();
      delete table.store.baseParams;
      table.store.baseParams = new Object();
      table.store.baseParams.root = node.attributes.extra;
      table.store.baseParams.level = node.attributes.qtip;
      table.store.baseParams.limit = table.bottomToolbar.pageSize;
      table.store.baseParams.mode = loader.baseParams.mode;
      table.store.load();
    }catch(e){
      alert('Error: ' + e.name + ': ' + e.message);
      return
    }
  }else{
    alert('Error: Can not find the "DataMonitoringTable" table');
    return
  }
}
function uniqueBookmark(title){
  var bkPanel = Ext.getCmp('bookmarkPanel');
  var result = bkPanel.store.find('Name',title);
  if(result >= 0){
    alert('Error: Bookmark with the same title ' + title + ' is already exists');
    return false
  }else{
    return true
  }
}
function bookmarkSync(mode,path,title){
  if((mode == null) || (mode == '')){
    alert('Error: mode is not defined');
    return false
  }
  var sideBar = Ext.getCmp('sideBar');
  var bookmarkPanel = Ext.getCmp('bookmarkPanel');
  var store = bookmarkPanel.getStore();
  if(mode == 'add'){
    if((path == null) || (path == '')){
      alert('Error: Bookmark path is not defined');
      return false
    }else if((title == null) || (title == '')){
      alert('Error: Bookmark title is not defined');
      return false
    }else{
      sideBar.body.mask('Creating bookmark...');
      var params = {'addBookmark':path,'addTitle':title};
    }
  }else if(mode == 'del'){
    if((path == null) || (path == '')){
      alert('Error: Bookmark path is not defined');
      return false
    }else if((title == null) || (title == '')){
      alert('Error: Bookmark title is not defined');
      return false
    }else{
      sideBar.body.mask('Deleting bookmark...');
      var params = {'delBookmark':path,'delTitle':title};
    }
  }else if(mode == 'get'){
    sideBar.body.mask('Refreshing bookmarks...');
    var params = {'getBookmarks':true};
  }else{
    alert('Error: mode ' + mode + ' is undefined');
    return false
  }
  Ext.Ajax.request({
    failure:function(response){
      sideBar.body.unmask();
      AJAXerror(response.responseText);
      return false
    },
    method:'POST',
    params:params,
    success:function(response){
      sideBar.body.unmask();
      var jsonData = Ext.util.JSON.decode(response.responseText);
      if(jsonData['success'] == 'false'){
        alert('Error: ' + jsonData['error']);
        return false
      }else{
        var serverResult = jsonData['result'];
        var result = []
        for(i in serverResult){
          result.push([i,serverResult[i]]);
        }
        store.loadData(result);
        return true
      }
    },
    url:'action'
  })
}
