function make_chnames(r2c)
    chnames_t = buildName("default",[r2c,makeRef("ch_scheme:5c3f333b783bba005cdfa6e2")]);
    chnames_slx_t = buildName("default",[r2c,makeRef("ch_scheme:5ea45b347ddd74004c800f22")]);
    chnames_s = buildName("default",[r2c,makeRef("ch_scheme:5c370367783bba005c825740")]);
    chnames_linreg = buildName("default",[r2c,makeRef("ch_scheme:5e4a65067ddd74005aa0030d")]);
    return chnames_t, chnames_slx_t, chnames_s, chnames_linreg
end

function makeRef(ref::String,id::String)
    return r2c=Dict("\$ref"=>ref,"\$id"=> Dict("\$oid"=> id))
end
function makeRef(s3ng::String)
    s3ng = split(s3ng,":")
    return makeRef(String(s3ng[1]),String(s3ng[2]))
end

function mongoPost(fun, data, cok)
  prx=getProxy();
  return mongoPost(fun, data, cok, prx)
end

function mongoPost(fun, data, cok, prx)
  ma = haskey(cok,"ma") ? cok["ma"] : "ma";
  #cks = Dict("ma_session" => cok["cma"], "ma_db" => cok["cdb"])

  headers = Dict("Cookie" => "ma_session = $(cok["cma"]); ma_db = $(cok["cdb"])")
  res = HTTP.request("POST", "http://$prx/$ma/$fun", headers, data);
  #res = Requests.post("http://$prx/$ma/$fun", data = data, cookies = cks)
  #res=readstring(res);
  res = JSON.parse(String(res.body));
  #res=JSON.parse(res);
  return res
end

function mongoGet(fun, data, cok)
  prx=getProxy();

  ma = haskey(cok,"ma") ? cok["ma"] : "ma";
  cks = Dict("ma_session" => cok["cma"], "ma_db" => cok["cdb"])
  res = HTTP.request("GET", "http://$prx/$ma/$fun"; verbose=3)
  res=readstring(res);
  res=JSON.parse(res);
  return res
end

function json_get(ref, key, cok="")
  data=JSON.json(Dict("ref"=> ref,"key"=> key))
  res = mongoPost("json_get", data, cok)
  return res["data"]["json"]
end

function json_set(ref, key, val,cok="")
  data= JSON.json(Dict("ref"=> ref,"key"=> key,"json"=>val))
  res = mongoPost("json_set", data, cok)
  return res
end

function ma_search(cok, prop)
  data=json(prop);
  res = mongoPost("search", data, cok)
  return res["data"]["result"]
end

function json_ref(ref,cok)
    data= JSON.json(Dict("ref"=> ref))
    res = mongoPost("json_ref", data, cok)
    res = res["data"]["list"]
    return [x["key"] for x in res]
end

function surface_ref(ref,cok)
    data= JSON.json(Dict("ref"=> ref))
    res = mongoPost("surface_ref", data, cok)
    res = res["data"]["list"]
    return [x for x in res]
end

function set_surface(r2c,dev_object_ref,surface_type_ref,date,grid,surface,cok="123")
  data=json(Dict("calc"=> r2c,
        "dev_object"=> dev_object_ref,
        "type"=> surface_type_ref,
        "date"=> date,
        "grid"=> grid,
        "surface"=> surface))
  res = mongoPost("surface_set", data, cok)
  return res
end

function get_surface(date, SuTyRef, calc_ref, dev_object_ref, cok="123")
  data=JSON.json(Dict("calc"=> calc_ref,              # Ссылка на расчёт
        "dev_object"=> dev_object_ref,  # Ссылка на объект разработки
        "type"=> SuTyRef,               # Ссылка на тип поверхности
        "date"=> date))                   # Дата
  #data = json(Dict("calc"=> calc_ref,"dev_object"=> dev_object_ref,"type"=> SuTyRef,"date"=> date))
  res = mongoPost("surface_get", data, cok)
  return res["data"]["obj"]["surface"]
end


function get_object(ref,cok="123")
    data = JSON.json(Dict("list"=>[ref]))
    res = mongoPost("get_objects", data, cok)
  return res["data"]["result"][1]
end

function getProxy()
  proxy = isdefined(Main, :proxy) ? Main.proxy : "proxy"
  return proxy
end
