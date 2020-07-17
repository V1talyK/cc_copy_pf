using HTTP, JSON
include("libs.jl")
include("clickHouse.jl")

proxy="192.168.1.60";
cok=Dict("cma"=>"0357a0f4-c110-41e1-b465-23cf8b964f3b","cdb"=>"","csql"=>"30","ch"=>"ch30");

r2c=makeRef("ccord_data:5f06b8be7ddd74004c7d8e74"); #Копируем (Ссылка на карточку с копированием)
obj=get_object(r2c, cok)["items"];
r_src = obj["src"]
r_dest= obj["dest"]

chnames_src = make_chnames(r_src)
chnames_dest = make_chnames(r_dest)

begin #Данные поскважинам
    capt=["id","Дата","Пл. дав. раб.","Пл. дав. откл.","Пл. дав. сред.",
    "Расч. заб. дав.","Дебит","Приёмистость","Коэф. пол. зак.","Статус","Скин","Коэф. связи","Гидропр.", "Ист. заб. дав.", "Ист. пл. дав."]
    filds=["wi","d","p1","p2","p3","pw","q","qz","kp","s","sk","ws","g","vf1", "vf2"]

    clm=Vector(undef, length(filds))
    sz=50*ones(length(capt)); sz[1]=16;
    for i=1:length(clm) clm[i]=Dict("field"=>filds[i],"caption"=> capt[i],"size"=>sz[i]);    end;
    merge!(clm[length(filds)],Dict("render"=> "toggle"))
    merge!(clm[1],Dict("hidden"=> true))

    cok["ch_db"] = "default";
    dropTlbInCH(chnames_dest[1],cok)
    genUTlb4CH(chnames_dest[1],cok, clm)

    pl = string("INSERT INTO $(chnames_dest[1]) SELECT * FROM $(chnames_src[1])");
    res = chPost(pl,cok);
end

begin #Салихов
    capt=["Индекс","Дата","Пл. дав.","Пл. дав. откл.","Заб.дав.","Cтатус","Скин","Коэф. связи с пластом","Гидропров."]
    filds=["wi","d","p1","p2","pw","s","sk","ws","g"]

    clm=Vector(undef, length(filds))
    for i=1:length(clm)
        clm[i]=Dict("field"=>filds[i],"caption"=> capt[i])
    end

    dropTlbInCH(chnames_dest[2],cok)
    genUTlb4CH(chnames_dest[2],cok, clm)

    pl = string("INSERT INTO $(chnames_dest[2]) SELECT * FROM $(chnames_src[2])");
    res = chPost(pl,cok);
end

begin #Поля
    dropTlbInCH(chnames_dest[3],cok)
    genTlbCH4Surf(chnames_dest[3], cok)

    pl = string("INSERT INTO $(chnames_dest[3]) SELECT * FROM $(chnames_src[3])");
    res = chPost(pl,cok);
end

begin #Регрессия
    filds = ["wi","a","b", "shift_b"]
    capt = ["id","a","b","shift_b"]

    clm=Vector(undef, length(filds))
    sz=50*ones(length(capt)); sz[1]=16;
    for i=1:length(clm) clm[i]=Dict("field"=>filds[i],"caption"=> capt[i],"size"=>sz[i]);    end;
    merge!(clm[length(filds)],Dict("render"=> "toggle"))
    merge!(clm[1],Dict("hidden"=> true))

    dropTlbInCH(chnames_dest[4],cok)
    genUSTlb4CH(chnames_dest[4],cok, clm)

    pl = string("INSERT INTO  $(chnames_dest[4]) SELECT * FROM $(chnames_src[4])");
    res = chPost(pl,cok);
end

#JSONы
json_list = json_ref(r_src,cok)
for (k,v) in enumerate(json_list)
    json_set(r_dest,v,json_get(r_src,v,cok),cok)
end
