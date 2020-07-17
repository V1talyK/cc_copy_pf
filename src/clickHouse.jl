function chPost(pl,cok)
    prx=getProxy();
    return chPost(pl,cok,prx)
end

function chPost(pl,cok,prx)
    ch = cok["ch"];
    # res=Requests.post("http://$prx/$ch/", data = pl)
    # res = readstring(res);
    #pl = JSON.json(pl)
    #println(pl)
    headers = Dict("Cookie" => "ma_session = $(cok["cma"]); ma_db = $(cok["cdb"])")
    #println("http://$prx/$ch/")
    res = HTTP.request("POST", "http://$prx/$ch/", headers, pl);
    if res.status!=200
        #if res[1:4]=="Code"
            println(res[1:100])
        #end;
    end;
    # println("----------")
    # println(String(res.body)=="")
    # println("----------")
    Sres = String(res.body);
    if Sres!=""
        return JSON.parse(Sres)
    else
        return Sres
    end
end

function chStreamPost(pl,cok)
    proxy=getProxy();
    ch = cok["ch"];
    s3m=Requests.post_streaming("http://$proxy/$ch/",
                                headers=Dict("Transfer-Encoding"=>"chunked"),
                                write_body=false)

    write_chunked(s3m, join(pl))
    #for data in pl
        #write_chunked(s3m, data)
    #end
    write_chunked(s3m, "")  # Signal that the body is complete
    return readstring(s3m)
end

function chTableExist(chname::String,cok)
    name, dbname = buildName(chname);
    pl = "EXISTS TABLE $dbname.$name FORMAT JSON"
    res = chPost(pl,cok);
    return res["data"][1]["result"]==1
end

function chTableExist(chname::String,cok,prx)
    name, dbname = buildName(chname);
    pl = "EXISTS TABLE $dbname.$name FORMAT JSON"
    res = chPost(pl,cok,prx);
    return res["data"][1]["result"]==1
end

function set_surf2CH(r2c,r2do,r2s,date,r2g,
                    A::Union{Array{Float32,1},Array{Float64,1}},cok,pref::String)
    #set_surface(r2c,r2do,r2s,string(v[t]),r2g,P[t,:],cok);
    #Сохраняем поверхность в ClickHouse
    dbname = get(cok,"ch_db","default");
    name=buildName(r2c,pref);
  return funSet_surf2CH(name, dbname,A,date,r2do,r2s,r2g,cok)
end

function set_surf2CH(chname::String,r2do,r2s,date,r2g,
                    A::Union{Array{Float32,1},Array{Float64,1}},cok)
    #set_surface(r2c,r2do,r2s,string(v[t]),r2g,P[t,:],cok);
    #Сохраняем поверхность в ClickHouse
    name, dbname = buildName(chname);
  return funSet_surf2CH(name, dbname,A,date,r2do,r2s,r2g,cok)
end

function funSet_surf2CH(name, dbname,A::Union{Array{Float32,1},Array{Float64,1}},date,r2do,r2s,r2g,cok)
    #Общая функция для сохранения 1 поверхности
    o,st,g = map(x->buildName(x),[r2do,r2s,r2g])
    A = convert(Array{Float64,1},A);
    #pl = "INSERT INTO $dbname.$name values ('$date', $g, $st, $o, $A)"
    pl = "INSERT INTO $dbname.$name values ('$date', '$g', '$st', '$o', $A)"
    #println(pl)
    res = chPost(pl,cok);
  return res
end

function set_surf2CH(r2c,r2do,r2s,vd,r2g,
                    A::Union{Array{Float32,2},Array{Float64,2}},cok,pref::String)
    #set_surface(r2c,r2do,r2s,string(v[t]),r2g,P[t,:],cok);
    #Сохраняем поверхности в ClickHouse
    dbname = get(cok,"ch_db","default");
    name=buildName(r2c,pref);
  return funSet_surf2CH(name, dbname,A,vd,r2do,r2s,r2g,cok)
end

function set_surf2CH(chname::String,r2do,r2s,vd,r2g,
                    A::Union{Array{Float32,2},Array{Float64,2}},cok)
    #set_surface(r2c,r2do,r2s,string(v[t]),r2g,P[t,:],cok);
    #Сохраняем поверхности в ClickHouse
    name, dbname = buildName(chname);
  return funSet_surf2CH(name, dbname,A,vd,r2do,r2s,r2g,cok)
end

function funSet_surf2CH(name, dbname,A::Union{Array{Float32,2},Array{Float64,2}},vd,r2do,r2s,r2g,cok)
    #Общая функция для сохранения поверхностей за временой интервал
    o,st,g = map(x->buildName(x),[r2do,r2s,r2g])
    A = convert(Array{Float64,2},A);
    pl = Vector{String}(undef, size(A,1))
    for i=1:size(A,1)
        vA = view(A,i,:);
        pl[i] = "('$(vd[i])', '$g', '$st', '$o', $vA)"
    end
    pl = string("INSERT INTO $dbname.$name values ",join(pl," "));
    res = chPost(pl,cok);
  return res
end

function passSurf2CH(r2c::Dict,r2do,r2s,vd,r2g,cok,PG, pref)
    #Пишем поверхности в кликхаус
    dropTlbInCH(r2c, cok,pref)
    genTlbCH4Surf(r2c, cok, pref)
    mn = 14;
    n = maximum([1 Int64(floor(mn/(sizeof(json(PG[1][1,:]))/1e6)))]);

    ns = Int64(floor(length(vd)/n));
    for i=1:ns
      v = (i-1)*n+(1:n)
      @sync begin
          for j=1:length(PG)
              @async set_surf2CH(r2c,r2do,r2s[j],vd[v],r2g,PG[j][v,:],cok, pref)
          end
      end
    end
     v = (ns*n+1):length(vd);
     @sync begin
         for j=1:length(PG)
             @async set_surf2CH(r2c,r2do,r2s[j],vd[v],r2g,PG[j][v,:],cok, pref)
        end
     end
end

function passSurf2CH(chname::String,r2do,r2s,vd,r2g,cok,PG)
    #Пишем поверхности в кликхаус
    dropTlbInCH(chname, cok)
    genTlbCH4Surf(chname, cok)
    mn = 14;
    n = maximum([1 Int64(floor(mn/(sizeof(json(PG[1][1,:]))/1e6)))]);

    ns = Int64(floor(length(vd)/n));
    for i=1:ns
      v = (i-1)*n.+(1:n)
      @sync begin
          for j=1:length(PG)
              @async set_surf2CH(chname,r2do,r2s[j],vd[v],r2g,PG[j][v,:],cok)
          end
      end
    end
     v = (ns*n+1):length(vd);
     @sync begin
         for j=1:length(PG)
             @async set_surf2CH(chname,r2do,r2s[j],vd[v],r2g,PG[j][v,:],cok)
        end
     end
end

function setSL2CH(r2c, wis, wie, date, XY, xy0)
    #setSL2CH = setStreamLine2ClickHouse
    #Сохраняем линии тока в ClickHouse
    proxy=getProxy();
    name=buildName(r2c,"sl");
    res=Vector(length(wis))
    for i=1:length(wis)
        wisi=wis[i];
        wiei=wie[i];
        X=convert(Array{Float64,1},XY[i][:,1]);
        Y=convert(Array{Float64,1},XY[i][:,2]);
        x0=xy0[i,1];
        y0=xy0[i,2];
        payload = "INSERT INTO default.$name values ($wisi, $wiei, '$date',
            $(xy0[i,1]), $(xy0[i,2]),  $X, $Y)"
        res[i]=post("http://$proxy/ch/", data = payload)
        res[i]=readstring(res[i])
    end

  return res
end

function setSL2CH2(chname, wis, wie, date, XY, xy0)
    #setSL2CH = setStreamLine2ClickHouse
    #Сохраняем линии тока в ClickHouse оптом
    proxy=getProxy();
    name, dbname = buildName(chname);
    p2=Vector(undef, length(wis))
    for i=1:length(wis)
        wisi=wis[i];
        wiei=wie[i];
        X=convert(Array{Float64,1},XY[i][:,1]);
        Y=convert(Array{Float64,1},XY[i][:,2]);
        x0=xy0[i,1];
        y0=xy0[i,2];
        p2[i] = "($wisi, $wiei, '$date', $(xy0[i,1]), $(xy0[i,2]),  $X, $Y)"
    end
    pl = string("INSERT INTO $dbname.$name values ",join(p2," "));
    res = chPost(pl,cok);
  return res
end

function setSL2CH2p(r2c::Dict, cok, wis, wie, date, XY, xy0, proxy,dbname)
    #setSL2CH = setStreamLine2ClickHouse
    #Сохраняем линии тока в ClickHouse оптом, при параллельном вызове
    ch = cok["ch"];
    name=buildName(r2c,"sl");
    p2=Vector(length(wis))
    for i=1:length(wis)
        wisi=wis[i];
        wiei=wie[i];
        X=convert(Array{Float64,1},XY[i][:,1]);
        Y=convert(Array{Float64,1},XY[i][:,2]);
        x0=xy0[i,1];
        y0=xy0[i,2];
        p2[i] = "($wisi, $wiei, '$date', $(xy0[i,1]), $(xy0[i,2]),  $X, $Y)"
    end
    pl = string("INSERT INTO $dbname.$name values ",join(p2," "));
    res = chPost(pl,cok);
  return res
end

function setSL2CH2p(chname::String, cok, wis, wie, date, XY, xy0, proxy)
    #setSL2CH = setStreamLine2ClickHouse
    #Сохраняем линии тока в ClickHouse оптом, при параллельном вызове по именитаблицы
    name, dbname = buildName(chname);
    p2=Vector(undef, length(wis))
    for i=1:length(wis)
        wisi=wis[i];
        wiei=wie[i];
        X=convert(Array{Float64,1},XY[i][:,1]);
        Y=convert(Array{Float64,1},XY[i][:,2]);
        x0=xy0[i,1];
        y0=xy0[i,2];
        p2[i] = "($wisi, $wiei, '$date', $(xy0[i,1]), $(xy0[i,2]),  $X, $Y)"
    end
    pl = string("INSERT INTO $dbname.$name values ",join(p2," "));
    res = chPost(pl,cok,proxy);
  return res
end

function get_surface_ch(chname::String, d, r2s, r2g, r2do, cok)
    #Получаем поверхность из ClickHouse
    proxy=getProxy();
  return get_surface_ch(chname, d, r2s, r2g, r2do, cok, proxy)
end

function get_surface_ch(chname::String, d, r2s, r2g, r2do, cok, proxy)
    #Получаем поверхность из ClickHouse
    st, g, o = map(x->buildName(x),[r2s, r2g, r2do])
    pl = "SELECT S FROM $chname WHERE d=='$d' AND t=='$st' AND g=='$g' AND o=='$o' FORMAT JSONCompact"
    res = chPost(pl,cok,proxy)
    #res = JSON.parse(res)
    res = res["data"][1][1]
  return res
end

function getSL4CH(r2c, pref="")
    #Получаем поверхность из ClickHouse
    proxy=getProxy();
    name=buildName(r2c,pref);
    # csql=cok["csql"];
    payload = "SELECT * FROM default.$name FORMAT JSON"
    display(payload)

    res=post("http://$proxy/ch/", data = payload)
    res=readstring(res)
    res=JSON.parse(res)
  return res
end

function getVPD4CH(r2c, pref="")
    #Получаем сохраненую табличку из ClickHouse
    proxy=getProxy();
    name=buildName(r2c,pref);
    # csql=cok["csql"];
    payload = "SELECT wi, d, psl FROM default.$name FORMAT JSONCompact"

    res=post("http://$proxy/ch/", data = payload)
    res=readstring(res)
    res=JSON.parse(res)
  return res
end

function genTaleInClickHouse()
    #Создаём таблицу в ClickHouse для хранения поверхностей
    proxy=getProxy();

    # payload = "CREATE TABLE default.vit2 (
    #   id UInt64,
    #   surface Array(Nullable(Float64))
    # ) ENGINE = Log"

    payload = "CREATE TABLE default.vit2 (
      id UInt64,
      d Date,
      surface Array(Nullable(Float64))
    ) ENGINE = MergeTree(d, (id, d), 8192)"

    #display(payload)
    res=post("http://$proxy/ch/", data = payload)
    res=readstring(res)
    #res=JSON.parse(res)
  return res
end

function genTlbCH4Sl(chname,cok)
    #genTlbCH4SL = genTaleInClickHouse
    #Создаём таблицу в ClickHouse для хранения линий тока без приставки имени
    name, dbname = buildName(chname);

    pl = "CREATE TABLE $dbname.$name (wis UInt64, wie UInt64, d Date,
        x0 Nullable(Float64), y0 Nullable(Float64),
        X Array(Nullable(Float64)), Y Array(Nullable(Float64)) )
        ENGINE = MergeTree(d, (wis, wie, d), 8192)"
    res = chPost(pl,cok);
  return
end

function genTlbCH4Sl(r2c, cok, dbname::String,pref::String)
    #genTlbCH4SL = genTaleInClickHouse
    #Создаём таблицу в ClickHouse для хранения линий тока с приставкой
    name=buildName(r2c,pref);

    pl = "CREATE TABLE $dbname.$name (wis UInt64, wie UInt64, d Date,
        x0 Nullable(Float64), y0 Nullable(Float64),
        X Array(Nullable(Float64)), Y Array(Nullable(Float64)) )
        ENGINE = MergeTree(d, (wis, wie, d), 8192)"

    res = chPost(pl,cok);
  return res
end

function genTlbCH4Surf(r2c, cok, pref::String)
    #Создаём таблицу в ClickHouse для хранения поверхностей с приставкой
    name=buildName(r2c,pref);
    dbname = get(cok,"ch_db","default");
    pl = "CREATE TABLE $dbname.$name (d Date, g String, t String,
        o String, S Array(Float64))
        ENGINE = MergeTree(d, (t, g, d), 8192)"

    res = chPost(pl,cok);
  return res
end

function genTlbCH4Surf(chname, cok)
    #Создаём таблицу в ClickHouse для хранения поверхностей с приставкой
    name, dbname = buildName(chname);
    pl = "CREATE TABLE $dbname.$name (d Date, g String, t String,
        o String, S Array(Float64))
        ENGINE = MergeTree(d, (t, g, d), 8192)"

    res = chPost(pl,cok);
  return res
end

function dropTlbInCH(chname,cok)
    #Удаляем таблицу в ClickHouse
    name, dbname = buildName(chname);
    pl = "DROP TABLE IF EXISTS $dbname.$name"
    res = chPost(pl,cok);
  return res
end

function dropTlbInCH(r2c, cok, pref::String)
    #Удаляем таблицу в ClickHouse с приставкой
    dbname = get(cok,"ch_db","default");
    name=buildName(r2c,pref);
    pl = "DROP TABLE IF EXISTS $dbname.$name"

    res = chPost(pl,cok);
  return res
end

function buildName(name::String)
    #ia = searchindex(name,".");
    ia = findfirst(".",name)
    ia = length(ia)>0 ? ia[1] : 0;
    dbname = ia!=0 ? name[1:ia-1] : "default";
    name = ia!=0 ? name[ia+1:end] : name;
    return name, dbname
end

function buildName(r2c::Dict)
    name=string("\"",r2c["\$ref"],":",r2c["\$id"]["\$oid"],"\"")
end

function buildName(r2c::Dict,syf::String)
    name=string("\"",r2c["\$ref"],":",r2c["\$id"]["\$oid"],".",syf,"\"")
end

function buildName(pref::String, r2c::Dict)
    name=string("\"",pref,".",r2c["\$ref"],":",r2c["\$id"]["\$oid"],"\"")
end
function buildName(dbname::String, v::Vector)
    v = map(x->string("_",x["\$ref"],"_",x["\$id"]["\$oid"]),v)
    name=string(dbname,".",string(v...))
end

function genUTlb4CH(r2c, cok, pref::String, clm)
    #genTlbCH4SL = genTaleInClickHouse
    #Создаём таблицу в ClickHouse для хранения восстановленных значений с пристакой
    dbname = get(cok,"ch_db","default");
    name=buildName(r2c, pref);
    return funGenUTlb4CH(clm,dbname,name,cok)
end

function genUTlb4CH(chname, cok, clm)
    #genTlbCH4SL = genTaleInClickHouse
    #Создаём таблицу в ClickHouse для хранения значений по имени из вне
    name, dbname = buildName(chname);
  return funGenUTlb4CH(clm,dbname,name,cok)
end

function funGenUTlb4CH(clm,dbname,name,cok)
    #Общая функция для GenUTlb4CH
    fld = Vector{String}(undef, length(clm));
    k=0;
    for i=1:length(clm)
        if clm[i]["field"]=="wi"

        elseif clm[i]["field"]=="d"
        elseif clm[i]["field"]=="wn"
            k+=1;
            fld[k] = string(clm[i]["field"], " String");
        else
            k+=1;
            fld[k] = string(clm[i]["field"], " Nullable(Float64)");
        end
    end
    fld = fld[1:k];
    lof = join(fld,", ")

    pl = "CREATE TABLE $dbname.$name (wi UInt64, d Date,
         $lof) ENGINE = MergeTree(d, (wi, d), 8192)"
    res = chPost(pl,cok);
  return res
end
function genUSTlb4CH(chname, cok, clm)
    #статическия таблица
    #Создаём таблицу в ClickHouse для хранения значений по имени из вне
    name, dbname = buildName(chname);
  return funGenUSTlb4CH(clm,dbname,name,cok)
end

function funGenUSTlb4CH(clm,dbname,name,cok)
    #Общая функция для GenUTlb4CH
    fld = Vector{String}(undef, length(clm));
    k=0;
    for i=1:length(clm)
        if clm[i]["field"]=="wi"
        elseif clm[i]["field"]=="wn"
            k+=1;
            fld[k] = string(clm[i]["field"], " String");
        else
            k+=1;
            fld[k] = string(clm[i]["field"], " Nullable(Float64)");
        end
    end
    fld = fld[1:k];
    lof = join(fld,", ")

    pl = "CREATE TABLE $dbname.$name (wi UInt64, $lof)
                    ENGINE = MergeTree() PARTITION BY (wi) ORDER BY (wi)"
    res = chPost(pl,cok);
  return res
end

function genWf4CH(chname, cok, fld)
    #genTlbCH4SL = genTaleInClickHouse
    #Создаём таблицу в ClickHouse для хранения значений по имени из вне
    name, dbname = buildName(chname);
  return funGenWf4CH(fld,dbname,name,cok)
end

function funGenWf4CH(fld,dbname,name,cok)
    #Общая функция для GenWf4CH
    lof = join(fld,", ")
    #println(lof)
    #pl = "CREATE TABLE $dbname.$name (wiI UInt64, wiP UInt64, d Date,
#         $lof) ENGINE = MergeTree(d, (wiI, wiP, d), 8192)"
    pl = "CREATE TABLE $dbname.$name (wiI UInt64, wiP UInt64, d Date,
                 $lof) ENGINE = MergeTree() PARTITION BY toYYYYMM(d)
                 ORDER BY(wiI, wiP, d) SETTINGS index_granularity=8192"

    #println(pl)
    res = chPost(pl,cok);
  return res
end

function setUTlb2CH(r2c, cok, pref::String, clm, rcd)
    #Сохраняем сохраняем универсальный json в ClickHouse
    dbname = get(cok,"ch_db","default");
    name=buildName(r2c,pref);
  return funSetUTlb2CH(clm,rcd,dbname,name,cok)
end

function setUTlb2CH(chname, cok, clm, rcd)
    #Сохраняем сохраняем универсальный json в ClickHouse по имени из вне
    name, dbname = buildName(chname);
  return funSetUTlb2CH(clm,rcd,dbname,name,cok)
end

function funSetUTlb2CH(clm,rcd,dbname,name,cok)
    fld = loop1getFld(clm);
    p2 = loop2Rcd(rcd,fld)

    pl = string("INSERT INTO $dbname.$name  values ",join(p2," "));
    res = chPost(pl,cok);
    #pl = string("INSERT INTO $dbname.$name  values ");
    #res = chStreamPost(vcat(pl,p2),cok)
   return res
end
function setUSTlb2CH(chname, cok, clm, rcd)
    #Сохраняем сохраняем универсальный json в ClickHouse по имени из вне
    name, dbname = buildName(chname);
  return funSetUSTlb2CH(clm,rcd,dbname,name,cok)
end

function funSetUSTlb2CH(clm,rcd,dbname,name,cok)
    fld = loop1getFld(clm);
    p2 = loop2SRcd(rcd,fld)

    pl = string("INSERT INTO $dbname.$name  values ",join(p2," "));
    res = chPost(pl,cok);
    #pl = string("INSERT INTO $dbname.$name  values ");
    #res = chStreamPost(vcat(pl,p2),cok)
   return res
end
function loop1getFld(clm)
    fld = Vector{String}(undef,length(clm));
    k=0;
    for i=1:length(clm)
        if clm[i]["field"]=="wi"

        elseif clm[i]["field"]=="d"
        elseif clm[i]["field"]=="wn"
        else
            k+=1;
            fld[k] = clm[i]["field"];
        end
    end
    fld = fld[1:k];
    return  fld
end

function loop2Rcd(rcd,fld)
    p2=Vector(undef, length(rcd))
    #for i=1:length(psl[3])
    for i=1:length(rcd)
        wi=rcd[i]["wi"];
        d=rcd[i]["d"];
        #wn=string(rcd[i]["wn"]);
        si = zeros(Float64, length(fld))
        for j=1:length(si)
            si[j] = haskey(rcd[i],fld[j]) ? rcd[i][fld[j]] : NaN;
        end
        s = join(si,", ")
        p2[i] = "($wi, '$d', $s)"
    end
    return p2
end

function loop2SRcd(rcd,fld)
    p2=Vector(undef, length(rcd))
    #for i=1:length(psl[3])
    for i=1:length(rcd)
        wi=rcd[i]["wi"];
        si = zeros(Float64, length(fld))
        for j=1:length(si)
            si[j] = haskey(rcd[i],fld[j]) ? rcd[i][fld[j]] : NaN;
        end
        s = join(si,", ")
        p2[i] = "($wi, $s)"
    end
    return p2
end

function setWfTlb2CH(chname, cok, rcd)
    #Сохраняем сохраняем универсальный json в ClickHouse по имени из вне
    name, dbname = buildName(chname);
  return funSetWflb2CH(rcd,dbname,name,cok)
end
function funSetWflb2CH(rcd,dbname,name,cok)
    #fld = loop1getFldX(clm);
    p2 = loop2RcdX(rcd)
    pl = string("INSERT INTO $dbname.$name  values ",join(p2," "));
    #println(pl)
    res = chPost(pl,cok);
    #pl = string("INSERT INTO $dbname.$name  values ");
    #res = chStreamPost(vcat(pl,p2),cok)
   return res
end

# function loop1getFldX(clm)
#     fld = Vector{String}(undef,length(clm));
#     k=0;
#     for i=1:length(clm)
#         if clm[i]["field"]=="wi"
#
#         elseif clm[i]["field"]=="d"
#         elseif clm[i]["field"]=="wn"
#         else
#             k+=1;
#             fld[k] = clm[i]["field"];
#         end
#     end
#     fld = fld[1:k];
#     return  fld
# end

function loop2RcdX(rcd)
    p2=Vector(undef, length(rcd))
    #for i=1:length(psl[3])
    for i=1:length(rcd)
        s = join(rcd[i],", ")
        p2[i] = "($s)"
    end
    return p2
end

function getMeta(Am)
    fname = Vector{String}(undef, length(Am));
    chty = Vector{String}(undef, length(Am));
    for i=1:length(Am)
        fname[i] = Am[i]["name"];
        chty[i] = Am[i]["type"];
    end
    return fname, chty
end

function getDS(r2c::Dict,cok,did,oid)
    #Получаем сохраненую табличку DS из ClickHouse
    lp = join(did, ", ")
    pl = "SELECT $lp FROM default.$r2c WHERE obj_id == $oid FORMAT JSONCompact"
    res = chPost(pl,cok)
    #res = JSON.parse(res)
    #res = readstring(res);
    return res
end
function getDS(r2c::Dict,cok,did,oid,ds,de)
    #Получаем сохраненую табличку DS из ClickHouse за временой интервал
    lp = join(did, ", ")
    pl = "SELECT $lp FROM default.$r2c WHERE obj_id == $oid AND date>='$ds' AND date<='$de' FORMAT JSONCompact"
    res = chPost(pl,cok)
    #res = JSON.parse(res)
    #res = readstring(res);
    return res
end

function getDS(chname::String,cok,did,ds,de)
    #Получаем сохраненую табличку DS из ClickHouse за временой интервал c произвольными фильтрами
    lp = join(did, ", ")
    pl = "SELECT $lp FROM $chname WHERE d>='$ds' AND d<='$de' FORMAT JSONCompact"
    res = chPost(pl,cok)
    #res = JSON.parse(res)
    #res = readstring(res);
    return res
end
function getDS(chname::String,cok,did,ds,de, wi)
    #Получаем сохраненую табличку DS из ClickHouse за временой интервал c произвольными фильтрами
    lp = join(did, ", ")
    list_wi = join(wi,",")
    pl = "SELECT $lp FROM $chname WHERE d>='$ds' AND d<='$de' AND wi in ($list_wi) FORMAT JSONCompact"
    return chPost(pl,cok)
end

function getDS(chname::String,cok,did,flt::Dict,ds,de)
    #Получаем сохраненую табличку DS из ClickHouse за временой интервал c произвольными фильтрами
    lp = join(did, ", ")
    flt = makeCHFilter(flt)
    pl = "SELECT $lp FROM $chname WHERE $flt d>='$ds' AND d<='$de' FORMAT JSONCompact"
    res = chPost(pl,cok)
    #res = JSON.parse(res)
    #res = readstring(res);
    return res
end

function getDS(chname::String,cok,did)
    #Получаем сохраненую табличку DS из ClickHouse за временой интервал c произвольными фильтрами
    lp = join(did, ", ")
    #flt = makeCHFilter(flt)
    pl = "SELECT $lp FROM $chname FORMAT JSONCompact"
    res = chPost(pl,cok)
    #res = JSON.parse(res)
    #res = readstring(res);
    return res
end

function makeCHFilter(flt)
    filt_eq = flt["eq"];   l1 = length(keys(filt_eq));
    filt_neq = flt["neq"]; l2 = length(keys(filt_neq));

    flt = Vector(undef, l1+l2)
    k=0
    for i=keys(filt_eq)
        k+=1;
        flt[k] = string(i,"==",filt_eq[i])
    end
    for i=keys(filt_neq)
        k+=1;
        flt[k] = string(i,"!=",filt_neq[i])
    end
    flt = join(flt," AND ");
    if length(flt)>0
        flt = string(flt," AND ")
    end

    return flt
end
