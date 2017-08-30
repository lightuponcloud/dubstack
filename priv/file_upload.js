var isMobile = {
    Android: function() {
        return navigator.userAgent.match(/Android/i);
    },
    BlackBerry: function() {
        return navigator.userAgent.match(/BlackBerry/i);
    },
    iOS: function() {
        return navigator.userAgent.match(/iPhone|iPad|iPod/i);
    },
    Opera: function() {
        return navigator.userAgent.match(/Opera Mini/i);
    },
    Windows: function() {
        return navigator.userAgent.match(/IEMobile/i);
    },
    any: function() {
        return (isMobile.Android() || isMobile.BlackBerry() || isMobile.iOS() || isMobile.Opera() || isMobile.Windows());
    }
};

function pad(number,length) {
    var str = '' + number;
    while (str.length < length) str = '0' + str;
    return str;
}
function unhex(obj_name){
 var bits = obj_name.split('/');
 for (var i=0, l=bits.length; i < l; i+=1){
  var chars="";
  var a=1;
  for (var j=0, cl = bits[i].length; j < cl; j += 2) {
    chars += '%'+bits[i].substring(j, j + 2);
  }
  try{
   bits[i] = decodeURIComponent(chars);
  } catch (e){
   if(e instanceof URIError) continue;
   throw e;
  }
 }
 return bits.join('/');
}

function get_query(a){
 a = a.substr(1).split('&');
 if (a == "") return {};
 var b = {};
 for (var i = 0; i < a.length; ++i){
  var p=a[i].split('=', 2);
  if (p.length == 1)
   b[p[0]] = "";
  else
   b[p[0]] = decodeURIComponent(p[1].replace(/\+/g, " "));
 }
 return b;
}

function display_objects(lstEl, brEl, hex_prefix, data, stack, embedded){
    var files=[];
    var directories=[];
    var riak_url=stack.get_riak_url();
    var root_uri=stack.get_root_uri();
    var token=stack.get_token();
    var bucket_name=stack.get_bucket_name();
    var prefix='';
    if(hex_prefix){
      prefix = unhex(hex_prefix);
      var bits = prefix.split('/');
      var emptyElidx = bits.indexOf("");
      if(emptyElidx!=-1) bits.splice(emptyElidx, 1);
      var hex_bits = hex_prefix.split('/');
      emptyElidx = hex_bits.indexOf("");
      if(emptyElidx!=-1) hex_bits.splice(emptyElidx, 1);
      var breadcrumbs = [];
      for(var i=0;i!=bits.length;i++){
	var part_prefix = hex_bits.slice(0, i+1).join('/');
	breadcrumbs.push({'part': bits[i], 'prefix': part_prefix});
      }
      $(brEl).empty();
      $(brEl).append('<a href="'+root_uri+token+'/'+bucket_name+'/">Root</a>');
      if(breadcrumbs.length>5){
        $(brEl).append('<span class="dirseparator"></span><span class="short">* * *</span>');
	breadcrumbs = breadcrumbs.slice(Math.max(breadcrumbs.length - 5, 1));
      }
      for(var i=0;i!=breadcrumbs.length-1;i++){
	var url = root_uri+token+'/'+bucket_name+'/?prefix='+breadcrumbs[i]['prefix'];
	var part_name = breadcrumbs[i]['part'];
	$(brEl).append('<span class="dirseparator"></span><a href="'+url+'">'+part_name+'</a>');
      }
      $(brEl).append('<span class="dirseparator"></span><span class="current">'+bits[bits.length-1]+'</span>');
      var prev_prefix = '';
      if(bits.length>1){
	var prev_prefix = hex_bits.slice(0, hex_bits.length-1).join('/');
      }
      directories.push({'name': '..', 'prefix': prev_prefix});
    }
    $(data.dirs).each(function(i,v){
      var dir_name = stack.array_integers_to_string(v);
      if(hex_prefix) {
	dir_name = dir_name.replace(hex_prefix, '');
      }
      directories.push({'name': dir_name, 'prefix': hex_prefix});
    });
    $(data.list).each(function(i,v){
      var date = v.last_modified;
      var name = stack.array_integers_to_string(v.name);
      var orig_name = stack.array_integers_to_string(v.orig_name);
      if(!embedded){
       files.push({'name': name, 'modified': date, 'orig_name': orig_name, 'short_url': v.short_url, 'preview_url': v.preview_url, 'public_url': v.public_url, 'ct': v.content_type, 'bytes': v.bytes});
      }
    });
    directories.sort();
    $(directories).each(function(i,v){
     var name = unhex(v.name);
     if(name.charAt(0)=="/") name=name.substr(1);
     var button='<div class="file-menu"><a class="menu-link" href="#" data-obj-n="'+encodeURIComponent(v.name)+'">Action<span>&#9660;</span></a></div>';
     var input='';
     var uri=root_uri+token+'/'+bucket_name+'/';
     var eff_prefix=v.prefix;
     if(v.name!='..'){
	if(hex_prefix){
	 eff_prefix = hex_prefix+v.name;
	} else {
	 eff_prefix = v.name;
	}
     } else {
	eff_prefix = v.prefix;
     }
     uri+='?prefix='+eff_prefix;
     var flink='<div class="file-name"><a title="'+(name=='..'?'Parent Directory':name)+'" class="file-link folder" href="'+uri+'"><i class="folder-icon"></i>'+name+'</a></div>';
     var fsize='<div class="file-size" data-bytes="0">&nbsp;</div>';
     if(embedded){
      button='';
      flink='<div class="dialog-file-name" data-prefix="'+eff_prefix+'"><a title="'+name+'" class="dialog-file-link folder" href="#" data-prefix="'+eff_prefix+'"><i class="folder-icon"></i>'+name+'</a></div>';
      fsize='';
     }
     $(lstEl).append('<div class="file-item dir '+(name=='..'?'parent-dir ':'')+'" style="display:block;" id="'+guid()+'">'+flink+fsize+'<div class="file-modified">&nbsp;</div><div class="file-preview-url"></div><div class="file-url"></div>'+(name=='..'?'<div class="file-menu"></div>':button)+'</div>');
    });
    files.sort();
    $(files).each(function(i,v){
     var name = stack.array_integers_to_string(v.orig_name);
     var obj_name = stack.array_integers_to_string(v.name);
     var fsize = filesize(v.bytes);
     var url = riak_url+'/'+bucket_name+'/'+obj_name;
     if(prefix) name = name.replace(prefix, '');
     var button='<div class="file-menu"><a class="menu-link" href="#" data-obj-n="'+encodeURIComponent(obj_name)+'" data-obj-url="'+url+'" data-obj-preview-url="'+v.preview_url+'" data-obj-pub-url="'+url+'">Action<span>&#9660;</span></a></div>';
     var input='';
     var d=new Date(v.modified*1000);
     var modified=pad(d.getDate(), 2)+'.'+pad(d.getMonth()+1,2)+'.'+d.getFullYear()+' '+pad(d.getHours(),2) + "." + pad(d.getMinutes(), 2)
     if(embedded) button='';
     $(lstEl).append('<div class="file-item clearfix" id="'+guid()+'">'+input+'<div class="file-name"><a title="'+name+'" class="file-link" href="#"><i class="doc-icon"></i>'+name+'</a></div><div class="file-size" data-bytes="'+v.bytes+'">'+fsize+'</div><div class="file-modified">'+modified+'</div><div class="file-preview-url">'+(v.preview_url==undefined?'&nbsp;':'&nbsp;')+'</div><div class="file-url"><a href="'+url+'">link</a></div>'+button+'</div>');
    });
}

function fetch_file_list(token, bucket_name, hex_prefix, msgEid, targetE, embedded){
  var root_uri=$('body').data('root-uri');
  var bucket_name=$('body').data('bucket-name');
  var stack = $.stack({'errorElementID': msgEid, 'loadingMsgColor': 'black', 'rpc_url': root_uri+'list/'+token+'/'+bucket_name+'/', 'onSuccess': function(data, status){
    $('#'+msgEid).empty();
    if(isMobile.any() || $(window).width()<670){
	$('#shadow').empty().hide();
    }else{
	$('.loading-message-wrap').hide();
    }
    var lst = $('#'+targetE);
    $(lst).empty();
    var directories=[];
    if(hex_prefix){
      var slash=hex_prefix.match(/\//g);
      var prev=(hex_prefix+'/').split('/', (slash==null?0:slash.length-1)).join('/');
      directories.push({'name': '..', 'modified': '', 'prefix': (prev==''?'':prev+'/')});
    }
    if(!embedded) {
     display_objects(lst, $('#id-block-header'), hex_prefix, data, stack, embedded);
     enable_menu_item('menu-createdir');
     refreshMenu();
    }else{
     display_objects(lst, $('#id-dialog-block-header'), hex_prefix, data, stack, embedded);
    }
    $('.files-tbl').show();
    if($(window).width()<670) {
	$(".file-link").width($('.file-item').width());
    }
  }, 'onFailure': function(data){
    $('#'+msgEid).empty();
    $('#'+msgEid).append('<span class="err">'+data.error+'</span>');
  }});
  stack.get_objects_list(hex_prefix);
}

function menuHack(e){
    var i = decodeURIComponent($(e).data('obj-n'));
    var lid = $(e).parent().parent().attr('id');
    var surl = $(e).data('obj-url');
    var purl = $(e).data('obj-pub-url');
    var rurl = $(e).data('obj-preview-url');
    $("#context-menu").data('obj-n', i);
    $("#context-menu").data('line-id', lid);
    $("#context-menu").data('obj-url', surl);
    $("#context-menu").data('obj-preview-url', rurl);
    $("#context-menu").data('obj-pub-url', purl);
    if(!surl) $('span#menu-copy-link').parent().replaceWith('<span class="lsp"><span id="menu-copy-link"><span>Copy Link</span></span></span>');
    if(!($(e).parent().parent().hasClass('dir'))){
	$("#menu-open span").text('Download');
	if($('#menu-preview').length==0&&isMobile.any()){
	  $("#menu-open").parent().parent().parent().append('<div class="row"><a href="'+rurl+'"><span id="menu-preview"><span>Preview</span></span></a></div>');
	  $('#menu-preview').click(function(){
	    $("#context-menu").hide();
	    $(".menu-link").hide();
	    $("#shadow").hide();
	    document.location = $(this).attr("href");
	  });
	}
    } else {
	$("#menu-open span").text('Open');
	if(isMobile.any()) $('#menu-preview').remove();
    }
}

function refreshMenu(){
 $(".file-size, .file-modified").click(function(){
  var href = $(this).parent().find(".file-link").attr('href');
  if(href!='#') document.location=href;
 });
 if(isMobile.any() || $(window).width()<670){
 $(".file-item").on('click', function(e){
    $(".file-item").removeClass("current");
    var el = $(this).find(".menu-link")[0];
    menuHack(el);
    var Fname = $(this).find(".menu-link").attr('data-obj-n');
    Fname = Fname.replace(/%20/g," ")
    if($("#context-menu").css('display') != 'block'){
	$(this).addClass("current");
	var lnk = $(this).find(".menu-link").attr('data-obj-pub-url');
	if(lnk == '#' || typeof lnk == 'undefined') lnk = $(this).find(".file-link").attr('href');
	var plnk = $(this).find(".menu-preview-link").attr('data-obj-preview-url');
	if(plnk == '#' || typeof plnk == 'undefined') plnk = $(this).find(".file-link").attr('href');
	$("#context-menu .open-link a").attr("href", lnk);
	$("#context-menu .open-preview-link a").attr("href", plnk);
	$("#context-menu .menu-head").find('span').text(Fname);
        $("#context-menu").show();
	$("#shadow").show();
    }
    return false; 
 });

 $("#context-menu .menu-head a").click(function(){
    $("#context-menu").hide();
    $("#shadow").hide();
    return false;
 });
 }else{
  $(".file-menu a").on('click', function(e){
    menuHack(this);
    if($("#context-menu").css('display') == 'block'){
        $("#context-menu").hide();
    }else{
        var topPos = $(this).offset().top - $(".management-block").offset().top + 30;
        var leftPos = $(this).offset().left  - $('.management-table').offset().left;
        $("#context-menu").css({top: topPos+'px',left: leftPos+'px'});
        $("#context-menu").show();
    }
    return false; 
  });
 }
 $(".file-item").hover(
    function(){
	if($("#context-menu").css('display') != 'block'){
	    $(this).find('.menu-link').show().css('display','inline-block');
	    $(this).closest('div.file-item').addClass('current');
	}
    },
    function(){
	if($("#context-menu").css('display') != 'block'){
	    $(this).find('.menu-link').hide();
	    $(this).closest('div.file-item').removeClass('current');
	}
    }
 );
 $('#context-menu a').click(function(e){
    $("#context-menu").hide();
    $(".menu-link").hide();

    var op=$(this).find('span').first().attr('id');
    var on=decodeURIComponent($(this).parent().parent().data('obj-n'));
    var lid=$("#context-menu").data('line-id');
    var surl=$(this).parent().parent().data('obj-url');
    var purl=$(this).parent().parent().data('obj-pub-url');
    var token = $("body").attr("data-token");
    var bucket_name = $("body").attr("data-bucket-name");
    var hex_prefix  = $("body").attr("data-hex-prefix");
    var root_uri=$('body').data('root-uri');

    if(op=='menu-copy'){
	copy_dialog(this, on, false);
	return false;
    } else if(op=='menu-move'){
	copy_dialog(this, on, true);
	return false;
    } else if(op=='menu-open'){
	$("#shadow").hide();
	document.location = '/'+surl;
    }else if(op=='menu-delete'){
	var msg='file';
	if($('#'+lid).hasClass('dir')) msg='directory';
	$('#id-dialog-obj-rm-msg').empty().append(msg+' <br/><b>'+$('#'+lid).find('.file-name a').text()+'</b>?');
	$(".confirm").show();
	$("#shadow").show();
	$(".current").addClass("deleting");

	$("#ok-btn").click(function(){
    	    var lid=$("#context-menu").data('line-id');
	    $(".confirm").hide();
	    $("#shadow").hide();

	    $('#'+lid).find('.menu-link').remove();
	    if($('#'+lid).hasClass('dir')){
		$('#'+lid).addClass('inactive');
	    }
	    var stack = $.stack({'errorElementID': 'id-status', 'rpc_url': root_uri+'delete/'+token+'/'+bucket_name+'/', 'onSuccess': function(data){
		$('#'+lid).remove();
		$('#id-status').empty();
	    }, 'onFailure': function(){
	        $('#'+lid).removeClass('inactive');
	        $('#id-status').empty();
	        $('#id-status').append('<span class="err">Error</span>');
	    }});
	    stack.delete_object(hex_prefix, decodeURIComponent($("#context-menu").data('obj-n')));
	    return false;
	});
	$("#cancel-btn").click(function(){
	    $(".confirm").hide();
	    $("#shadow").hide();
	    $(".file-item").removeClass("deleting");
	    return false;
	});
	$('.deleting a').click(function(e){
	    e.preventDefault();
	    return false;
	});
    } else if(op=='menu-copy-link'){
     var link_d = '<input type="text" id="id-link-cppy" value="/'+surl+'" name="linkcopy">';
     $("#dialog").dialog({
      title: 'Short Link',
      resizable: false,
      height: 'auto',
      width:'300',
      position: [($(window).width() / 2) - (600 / 2), 150],
      modal: true,
      draggable: false,
      open: function (event, ui) {
       $(this).empty().append(link_d).find('#id-link-copy').select();
      },
      close: function (event, ui) {
        event.stopPropagation();
        event.preventDefault();
        $(this).dialog("close");
      }
     });
     $("#dialog").dialog('open');
    }
    return false;
 });
}

function guid() {
  function s4() {
    return Math.floor((1 + Math.random()) * 0x10000)
      .toString(16)
      .substring(1);
  }
  return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
    s4() + '-' + s4() + s4() + s4();
}

function copy_dialog(e, from_object_name, to_move){
 var token = $("body").attr("data-token");
 var bucket_name = $("body").attr("data-bucket-name");
 var copy_form = '<form method="POST" id="id-copy-form" name="copy_form" > \
<input type="hidden" name="hex_prefix" id="id-dialog-prefix" value=""> \
<h3><span id="id-dialog-block-header"></span></h3> \
<div class="files-tbl clearfix"><div class="files-tbl_th"> \
<div class="file-name_th"><a href="#" id="id-dialog-sort-by-name" data-sorting="asc">Name</a></div> \
<div class="file-size_th"><a href="#" id="id-dialog-sort-by-size" data-sorting="asc">Size</a></div> \
<div class="file-modified_th"><a href="#" id="id-dialog-sort-by-date" data-sorting="asc">Modified</a></div> \
<div></div></div><div id="id-dialog-objects-list"></div></div> \
<br/><br/><span id="id-copy-distination"></span>&nbsp;&nbsp;<span class="pushbutton"><button type="submit" id="submit_copy">'+(to_move==true?'Move':'Copy')+'</button></span> \
<span id="id-dialog-loading-message-text"></span>';

 var title='Copy. Chose Destination Directory';
 if(to_move){
  title='Move. Chose Destination Directory';
 }
 $("#dialog").dialog({
  title: title,
  autoOpen: false,
  resizable: false,
  height: 'auto',
  width:'600',
  position: [($(window).width() / 2) - (600 / 2), 150],
  modal: true,
  draggable: false,
  open: function (event, ui) {
   $(this).empty().append(copy_form);
   $('#id-block-header *').clone().appendTo($('#id-dialog-block-header'));
   $('#id-dialog-block-header').find('.current').css('color', 'black');
   $('#id-objects-list div.file-item').each(function(i,v){
    if($(v).hasClass('inactive')||!$(v).hasClass('dir')) return false;
    $(v).clone().appendTo($('#id-dialog-objects-list'));
   });
   $('#id-dialog-objects-list').show();
   $('#id-dialog-objects-list').find('div.file-menu').remove();
   $('#id-dialog-objects-list').find('div.file-size').remove();
   $('#id-dialog-objects-list').find('div.file-url').remove();
   $('#id-dialog-objects-list').find('div.file-preview-url').remove();
   $('#id-dialog-objects-list').parent().find('div.file-size_th').remove();
   $('#id-dialog-objects-list .file-item').each(function(i,v){
    var fn=$(v).find('.file-name');
    var args=get_query($(v).find('.file-link')[0].search);
    var prefix=args.prefix;
    $(v).find('.file-link').data('prefix', prefix);
    $(v).find('.file-link').attr('href', '#');
    $(v).find('.file-link').removeClass('file-link').addClass('dialog-file-link');
    $(v).find('.file-name').removeClass('file-name').addClass('dialog-file-name');
    $(v).attr('id', guid());
   });
   $("#id-dialog-objects-list").on('click', '.dialog-file-name', function(e){
    var hex_prefix=$(this).find('a.dialog-file-link').data('prefix');
    $('#id-dialog-prefix').val(hex_prefix);
    $('#id-dialog-block-header .current').empty();
    fetch_file_list(token, bucket_name, hex_prefix, 'id-dialog-loading-message-text', 'id-dialog-objects-list', true);
    return false;
   });
   if($(window).width()<670){
    $("#dialog").dialog("option", "width", 300);
    $("#dialog").dialog("option", "position", { my: "center", at: "center", of: window});
   }
   $(window).resize(function(){
       if($(window).width()<670){
	$("#dialog").dialog( "option", "width", 300 );
      }else{
	$("#dialog").dialog( "option", "width", 600 );
      }
      $("#dialog").dialog( "option", "position", { my: "center", at: "center", of: window });
   });
  },
  close: function (event, ui) {
    event.stopPropagation();
    event.preventDefault();
    $("#shadow").hide();
    $(this).dialog("close");
  }
 });
 $("#dialog").dialog('open');

 $('#submit_copy').click(function(){
  $("#submit_copy").attr("disabled","disabled");
  var root_uri=$('body').data('root-uri');
  var token=$('body').data('token');
  var src_bucket_name=$('body').data('bucket-name');
  var src_hex_prefix=$('body').data('hex-prefix');
  var dst_hex_prefix = $('#id-dialog-prefix').val();
  var rpc_url = root_uri+'copy/'+token+'/'+bucket_name+'/';
  if(to_move) rpc_url = root_uri+'move/'+token+'/'+bucket_name+'/';
  var stack = $.stack({'errorElementID': 'id-status', 'rpc_url': rpc_url, 'onSuccess': function(data){
    $('#id-status').empty();
    $("#submit_copy").attr("disabled","");
    $("#dialog").dialog('close');
   }
  });
  stack.copy_object(src_bucket_name, src_hex_prefix, from_object_name, src_bucket_name, dst_hex_prefix, from_object_name);
//  var objects=[[src_hex_prefix, from_object_name]];
// TODO: to check status of copied objects
  return false;
 });
 return false;
}

function disable_menu_item(menu_item_id){
 if(menu_item_id=='menu-createdir'){
  $('#id-createdir-button').attr('disabled', 'disabled');
 } else if(menu_item_id=='id-upload-button'){
  $('#id-upload-button').attr('disabled', 'disabled');
 }
}
function enable_menu_item(menu_item_id){
 if(isMobile.any() || $(window).width()<670){$("#shadow").css('z-index', 9).hide();}
 if(menu_item_id=='menu-createdir'){
  $('#id-createdir-button').removeAttr('disabled');
 } else if(menu_item_id=='menu-upload'){
  $('#id-upload-button').removeAttr('disabled');
 }

}

function submit_dirname(hex_prefix){
 var token = $("body").attr("data-token");
 var bucket_name = $("body").attr("data-bucket-name");
 var root_uri=$('body').data('root-uri');

 $('#id-status').empty();
 $('#id_directory_name_errors').empty();
 $('#id_submit_createdir').attr("disabled", true);
 if(isMobile.any() || $(window).width()<670){$("#shadow").append('<span>Two moments please...</span>').css('z-index','1003').show();}
 disable_menu_item('menu-createdir');

 var directory_name = $('#id_directory_name').val().replace(/^\s+|\s+$/gm,'');
 $('#id_directory_name_errors').empty();
 if(name.match("/\./|/\.\./|/\.$|/\.\.$")){
   $('#id_directory_name_errors').append(gettext('Name can\'t contain path.'));
   $('#id_submit_createdir').attr("disabled", false);
   enable_menu_item('menu-createdir');
   return;
 }
 if(name.length>50){
   $('#id_directory_name_errors').append('<br/>'+gettext('The length should not exceed 50 characters.'));
   $('#id_submit_createdir').attr("disabled", false);
   enable_menu_item('menu-createdir');
   return;
 }
 var fc=["'", '"', "`", "<", ">"];
 for(var i=0;i!=fc.length;i++){
   if(name.indexOf(fc[i])!=-1){
    $('#id_directory_name_errors').append(gettext('Forbidden character: <b>'+fc[i]+'</b>.'));
    $('#id_submit_createdir').attr("disabled", false);
    enable_menu_item('menu-createdir');
    return;
   }
 }
 var stack = $.stack({'errorElementID': 'id-status',
	'rpc_url': root_uri+'create-pseudo-directory/'+token+'/'+bucket_name+'/',
	'onSuccess': function(data){
    $("#shadow").empty().css('z-index', 9).hide();
    enable_menu_item('menu-createdir');
    $('#id_submit_createdir').attr("disabled", false);
    fetch_file_list(token, bucket_name, hex_prefix, 'id-loading-message-text', 'id-objects-list');
    enable_menu_item('menu-createdir');
    $('.ui-dialog-titlebar-close').click();
    $('#id-status').empty();
 }, 'onFailure': function(data){
    if(data.error&&data.error.indexOf('Traceback')!=-1){
     $('#id_directory_name_errors').append(gettext('Infrastructure Controller returned error.'));
    } else {
     $('#id_directory_name_errors').append(data.error);
    }
    enable_menu_item('menu-createdir');
    $('#id_submit_createdir').attr("disabled", false);
 }});
 stack.directory_create(hex_prefix, directory_name);
}

function sort_table(column_class, order){
 var table = $('#id-objects-list');
 var asc = order === 'asc';
 table.find('.file-item').sort(function(a, b) {
  var va=$(column_class, a).text();
  var vb=$(column_class, b).text();
  if(column_class=='.file-size'){
   va=$(column_class, a).data('bytes');
   vb=$(column_class, b).data('bytes');
  }
  if (asc) {
    if(column_class=='.file-size'){return va > vb;}else{return va.localeCompare(vb);}
  } else {
    if(column_class=='.file-size'){return vb > va;}else{return vb.localeCompare(va);}
  }
 }).appendTo(table);
 $('.parent-dir').detach().insertBefore('#id-objects-list .file-item:first');
}

$(".cancel-btn").on('click', function(){
    $('#shadow, #popup').css('display', 'none');
    return false;
});

function upload_files(token, bucket_name, hex_prefix, files, upload_ids){
  var file = files.shift();
  var upload_id = upload_ids.shift();
  var root_uri=$('body').data('root-uri');

  var stack = $.stack({'errorElementID': 'id-progress_'+upload_id, rpc_url: root_uri+'upload/'+token+'/'+bucket_name+'/', 'chunk_size': 2000000, 'onSuccess': function(data, status){
   if(data.hasOwnProperty('error')){
      stack.parse_file_upload_error(upload_id, data['error']);
      return;
   };
   if(files){
    upload_id=upload_ids.shift();
    file = files.shift();
    if(file){
      stack.file_upload(file, upload_id, hex_prefix);
    } else {
      $('#id-upload-button').data('uploadDisabled', false);
      if(hex_prefix){
	  fetch_file_list(token, bucket_name, hex_prefix, 'id-loading-message-text', 'id-objects-list');
	} else {
	  fetch_file_list(token, bucket_name, NaN, 'id-loading-message-text', 'id-objects-list');
	}
    };
   }
  }, 'onProgress': function(evt, offset, file_size){
    if (evt.lengthComputable) {
      if(typeof(offset)=="number"&&typeof(file_size)=="number"){
	var p=parseInt(((offset+evt.loaded) / file_size * 100), 10);
      } else {
	var p=parseInt((evt.loaded / evt.total * 100), 10);
      }
      if(p==100) p = '<span style="color:green;">'+p+'%</span>';
	else p = p+'%';
	$('#id-progress_'+upload_id).empty().append(p);
      }
  }, 'onFailure': function(xhr, status, msg){
    $('#id-upload-button').data('uploadDisabled', false);
    if(msg=='fd_error'){
      $('#id-progress_'+upload_id).empty().append('<span class="err">file read error</span>');
      return;
    }else if((status=='error'||status=='timeout')&&xhr.readyState==0){
	var attempts=$('#id-progress_'+upload_id).data('attempts');
	if(attempts==undefined) attempts=5;
	attempts-=1;
	$('#id-progress_'+upload_id).empty().data('attempts', attempts);
	if(attempts<0){
	  $('#id-progress_'+upload_id).empty().append('<span class="err">'+gettext("connection timeout")+'</span>');
	  return;
	}
	$('#id-progress_'+upload_id).empty().append('<span class="err">'+gettext('network error, retrying')+'</span>');
	setTimeout(function(){
	    $('#id-progress_'+upload_id).empty().append('queueing');
	    upload_files(token, bucket_name, hex_prefix, [file], [upload_id]);
	}, 3000);
      return;
    }else if(msg.hasOwnProperty('error')){
      stack.parse_file_upload_error(upload_id, msg['error']);
      return;
    } else if(status=='error'){
      $('#id-progress_'+upload_id).empty().append('<span class="err">error, try later</span>');
      return;
    }
  }
 });
 stack.file_upload(file, upload_id, hex_prefix);
}

function upload_dialog_add_file(file){
 var fsize_bytes = file.size;
 var fuid=fsize_bytes+file.lastModified;
 if($('#id-dialog-objects-list').find('#'+fuid).length>0) return false;
 var fsize = filesize(fsize_bytes);
 var upload_id=guid();
 $('#id-dialog-objects-list').append('<div class="file-item clearfix" id="'+upload_id+'"><div class="file-name" id="'+fuid+'"><a title="'+file.name+'" class="file-link" href="#"><i class="doc-icon"></i>'+file.name+'</a></div><div class="file-size" data-bytes="'+fsize_bytes+'">'+fsize+'</div><div><div id="id-progress_'+upload_id+'" data-attempts="5">queueing</div></div><div></div></div>');
 return upload_id;
}


$(document).on('dragenter', function(e){e.stopPropagation();e.preventDefault();});
$(document).on('dragover', function(e){e.stopPropagation();e.preventDefault();});
$(document).on('drop', function(e){e.stopPropagation();e.preventDefault();});

$(document).ready(function(){
 if (!window.File || !window.FileReader || !window.FileList || !window.Blob) {
  disable_menu_item('id-upload-button');
  $('#id-upload-button').attr('title', 'Your browser do not support HTML5');
 }

 if(isMobile.any() || $(window).width()<670){
    $('.loading-message-wrap').hide();
    $('#shadow').append('<span>Loading...</span>').show();
 }

 $(document).keydown(function(e){
    if (($("#context-menu").css('display') == 'block') && (e.which == 27 || e.keyCode == 27)){
	$("#context-menu").hide();
	$(".file-item").removeClass("current");
	if($("#shadow").css("display")=='block') $("#shadow").hide();
	$(".menu-link").hide();
    }
 });
 $(document).mousedown(function(e) {
    if (
	$("#context-menu").css('display') == 'block'
	&& e.which == 1  
	&& ($(e.target).closest('div').parent().attr('id')!='context-menu') 
	&& !($(e.target).hasClass("menu-link"))
	&& !($(e.target).parent().hasClass("menu-link"))
       )
    {
	$("#context-menu").hide();
	$(".file-item").removeClass("current");
	$(".menu-link").hide();
	if($("#shadow").css("display")=='block') $("#shadow").hide();
    }
 });

 var hex_prefix = $("body").attr("data-hex-prefix");
 var token = $("body").attr("data-token");
 var bucket_name = $("body").attr("data-bucket-name");
 if(hex_prefix){
  fetch_file_list(token, bucket_name, hex_prefix, 'id-loading-message-text', 'id-objects-list');
 } else {
  fetch_file_list(token, bucket_name, NaN, 'id-loading-message-text', 'id-objects-list');
 }

 $('.files-tbl').on('click', 'a.file-link:not(.folder)', function(){return false;})

 $('span.pushbutton').on('click', '#id-upload-button', function(){

 var u_htm = '<form id="id-dialog-upload-form" name="upload_form" enctype="multipart/form-data" method="post" > \
<div class="drop-area"> \
<span class="fileinput-button">	<span class="btn btn-green fileinput-button"><span>Add files..</span><input type="file" name="files[]" multiple="" id="id-dialog-file"></span></span> \
<span class="err" id="id_upload_errors"></span><div class="files-tbl clearfix" style="display:none;" id="id-dialog-objects-list-wrapper"><div class="files-tbl_th"> \
<div class="file-name_th"><a href="#" id="id-dialog-sort-by-name" data-sorting="asc">Name</a></div> \
<div class="file-size_th"><a href="#" id="id-dialog-sort-by-size" data-sorting="asc">Size</a></div> \
<div class="file-modified_th"><a href="#" id="id-dialog-progress">&nbsp;</a></div> \
<div></div></div><div class="clearfix" id="id-dialog-objects-list"></div></div></div></form>';
 $("#dialog").dialog({
    title: 'Drag Files to the area below or press button to add files',
    autoOpen: false,
    resizable: false,
    height: 'auto',
    maxHeight: 600,
    width:'700',
    show: { effect: 'drop', direction: "up" },
    modal: true,
    draggable: true,
    open: function (event, ui){
        $(this).empty().append(u_htm);
    },
    close: function (event, ui){
        $("#dialog").dialog().dialog('close');
    }
 });
 $("#dialog").dialog('open');

  var da=$('.drop-area');
  da.on('dragenter', function(e){
    e.stopPropagation();e.preventDefault();$(this).css('border', '2px solid #0B85A1');
  });
  da.on('dragover', function(e){e.stopPropagation();e.preventDefault();});
  da.on('drop', function(e){
   $(this).css('border', '2px dotted #0B85A1');
   e.preventDefault();
   var d_files=new Array();
   var d_upload_ids=[];
   $.each(e.originalEvent.dataTransfer.files, function(i, file){
     d_upload_ids.push(upload_dialog_add_file(file));
     d_files.push(file);
   });
   $('#id-dialog-objects-list-wrapper').show();
   upload_files(token, bucket_name, hex_prefix, d_files, d_upload_ids);
  });

 $('#id-dialog-file').on('change', function(evnt){
  var files=new Array();
  var upload_ids=[];
  var hex_prefix = $("body").attr("data-hex-prefix");
  var token = $("body").attr("data-token")
  var bucket_name = $("body").attr("data-bucket-name")
  $.each(this.files, function(i, file){
    upload_ids.push(upload_dialog_add_file(file));
    files.push(file);
  });
  $('#id-dialog-objects-list-wrapper').show();

  if(files.length==0) return;
  var wait = function(){
    var is_disabled = $('#id-upload-button').data('uploadDisabled');
    if(is_disabled) {
      setTimeout(wait, 2000);
    } else {
      $('#id-upload-button').data('uploadDisabled', true);
      upload_files(token, bucket_name, hex_prefix, files, upload_ids);
    }
  };
  wait();
 });
 return false;
});

$('span.pushbutton').on('click', '#id-createdir-button', function(){
 var currentElement=$(this);
 var hex_prefix=$("body").attr("data-hex-prefix");
 var d_htm = '<form action="" id="id_createdir_form" name="createdir_form" > \
<br/><input name="directory_name" id="id_directory_name"/> \
<span class="pushbutton"><button type="button" id="id_submit_createdir">Submit</button></span> \
<span class="err" id="id_directory_name_errors"></span></form>';
 $("#dialog").dialog({
        title: 'New Directory Name',
        autoOpen: false,
        resizable: false,
        height: 'auto',
	width:'40%',
        show: { effect: 'drop', direction: "up" },
        modal: true,
        draggable: true,
	create: function( event, ui ) {
	    // Set maxWidth
	    $(this).closest('.ui-dialog').css("maxWidth", "400px");
	},
        open: function (event, ui) {
            $(this).empty().append(d_htm);
        },
        close: function (event, ui) {
            $("#dialog").dialog().dialog('close');
	    $("#shadow").empty().hide();
        }
 });
 $("#dialog").dialog('open');
 $('#id_createdir_form').submit(function(e){
  e.preventDefault();e.stopImmediatePropagation();
  submit_dirname(hex_prefix);
 });
 $('#id_directory_name').focus();
 $('#id_submit_createdir').click(function(){
  submit_dirname(hex_prefix);
  return false;
 });
 return false;
});

$('.files-tbl').on('click', 'a#id-sort-by-name', function(){
 var order=($(this).data('sorting')=='asc')?'dsc':'asc';
 sort_table('.file-name', order);
 $(this).data('sorting', order);
 return false;
});

$('.files-tbl').on('click', 'a#id-sort-by-date', function(){
 var order=($(this).data('sorting')=='asc')?'dsc':'asc';
 sort_table('.file-modified', order);
 $(this).data('sorting', order);
 return false;
});

$('.files-tbl').on('click', 'a#id-sort-by-size', function(){
 var order=($(this).data('sorting')=='asc')?'dsc':'asc';
 sort_table('.file-size', order);
 $(this).data('sorting', order);
 return false;
});

$('#obj_search').autocomplete({
  source: function(req, responseFn) {
    $.ajax({
     url: "/internal_solr/binary_objects/suggest",
     cache: false,
     dataType: 'json',
     data: {
      q: req.term,
      wt: "json"
     },
     success: function(data, status) {
	var result = [];
	$(data['grouped']['orig_name']['groups']).each(function(i,v){
	  result.push(v.groupValue)
	});
        responseFn( result );
     }
    });
  },
  minLength: 1
});

var search_form = $('#search_form').ajaxForm({
   success: function(data){
     $('#search_form input[type=button]').removeAttr("disabled");
     if(isMobile.any() || $(window).width()<670){
	$('#shadow').empty().hide();
     }else{
	$('.loading-message-wrap').hide();
     }
     var lst = $('#id-objects-list');
     var directories=[];
     var embedded=false;
     var stack = $.stack();
     $(lst).empty();
     if(!embedded) {
      display_objects(lst, $('#id-block-header'), NaN, data, stack, embedded);
      enable_menu_item('menu-createdir');
      refreshMenu();
     } else {
      display_objects(lst, $('#id-dialog-block-header'), NaN, data, stack, embedded);
     }
     $('.files-tbl').show();
     if($(window).width()<670) {
	$(".file-link").width($('.file-item').width());
     }
     return false;
   }
});

$('#search_form input[type=button]').click(function(){
 $('#search_form input[type=button]').attr("disabled","");
 search_form.submit();
 return false;
});

});
