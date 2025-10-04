import pandas as pd 
from datetime import datetime
import time 
import re 


ticker_keywords = {
    'CBOM': [r'московск\w*\s*кредитн\w*\s*банк\w*', r'moscow.credit.bank\w*'],
    'ALRS': [r'алрос\w*', r'alrosa\w*'],
    'VTBR': [r'втб\w*', r'vtb\w*'],
    'MDMG': [r'мд\s*медикал\w*', r'md.medical\w*'],
    'GEMC': [r'юнайтед\s*медикал\w*', r'united.medical\w*'],
    'VKCO': [r'вк\s', r'vk\w*'],
    'LENT': [r'лента\w*', r'lenta\w*'],
    'RUAL': [r'русал\w*', r'rusal\w*'],
    'T': [r'т-технологи\w*'],
    'HEAD': [r'хэдхантер\w*', r'headhunter\w*'],
    'CNRU': [r'циан\w*', r'cian\w*'],
    'ENPG': [r'эн\s*груп\w*', r'en+.group\w*'],
    'YDEX': [r'яндекс\w*', r'yandex\w*'],
    'BSPB': [r'банк\s*санкт-петербург\w*', r'bank.st.petersburg\w*'],
    'AQUA': [r'инарктика\w*', r'inarktika\w*'],
    'AFKS': [r'система\w*', r'afks\w*', r'акционерн\w*\s*финансов\w*\s*корпорац\w*'],
    'AFLT': [r'аэрофлот\w*', r'aeroflot\w*'],
    'VSEH': [r'ви\.ру\w*', r'vi.ru\w*'],
    'GAZP': [r'газпром\w*', r'gazprom\w*'],
    'GMKN': [r'норникел\w*', r'norilsk\w*', r'гмк\w*норникел\w*', r'норильск\w*'],
    'RAGR': [r'русагро\w*', r'rusagro\w*'],
    'LSRG': [r'группа\s*лср\w*', r'lsr.group\w*'],
    'POSI': [r'группа\s*позитив\w*', r'positive.group\w*'],
    'RENI': [r'ренессанс\s*страхован\w*', r'renaissance.insurance\w*'],
    'EUTR': [r'евротранс\w*', r'eurotrans\w*'],
    'IRAO': [r'интер\s*рао\w*', r'inter.rao\w*'],
    'X5': [r'икс\s*5\w*', r'x5\w*'],
    'LEAS': [r'европлан\w*', r'europlan\w*'],
    'MVID': [r'м\.видео\w*', r'm.video\w*'],
    'MBNK': [r'мтс-банк\w*', r'mts.bank\w*'],
    'MAGN': [r'ммк\w*', r'mmk\w*', r'магнитогорск\w*\s*металлургическ\w*'],
    'MTLR': [r'мечел\w*', r'mechel\w*'],
    'MTLRP': [r'мечел\w*', r'mechel\w*'],
    'MTSS': [r'мтс\w*', r'mts\w*', r'мобильн\w*\s*телесистем\w*'],
    'MOEX': [r'московск\w*\s*бирж\w*', r'moscow.exchange\w*', r'мосбирж\w*'],
    'LKOH': [r'лукойл\w*', r'lukoil\w*'],
    'BELU': [r'новабев\w*', r'novabev\w*'],
    'NLMK': [r'нлмк\w*', r'nlmk\w*', r'новолипецк\w*'],
    'PIKK': [r'пик\s', r'pik\w*'],
    'PLZL': [r'полюс\w*', r'polyus\w*'],
    'RTKM': [r'ростелеком\w*', r'rostelecom\w*'],
    'RTKMP': [r'ростелеком\w*', r'rostelecom\w*'],
    'SBER': [r'сбербанк\w*', r'sberbank\w*', r'сбер\w*'],
    'SBERP': [r'сбербанк\w*', r'sberbank\w*', r'сбер\w*'],
    'CHMF': [r'северстал\w*', r'severstal\w*'],
    'SELG': [r'селигдар\w*', r'seligdar\w*'],
    'SVCB': [r'совкомбанк\w*', r'sovcombank\w*'],
    'FLOT': [r'современн\w*\s*коммерческ\w*\s*флот\w*', r'sovcomflot\w*'],
    'TGKA': [r'тгк\s*1\w*', r'tgk.1\w*'],
    'TRNFP': [r'транснефт\w*', r'transneft\w*'],
    'HYDR': [r'русгидро\w*', r'rushydro\w*'],
    'FEES': [r'россети\w*', r'rosseti\w*'],
    'FIXR': [r'фикс\s*прайс\w*', r'fix.price\w*'],
    'PHOR': [r'фосагро\w*', r'phosagro\w*'],
    'ELFV': [r'эл5-энерго\w*', r'el5.energo\w*'],
    'SFIN': [r'эсэфай\w*', r'sfai\w*'],
    'UPRO': [r'юнипро\w*', r'unipro\w*'],
    'ASTR': [r'астра\w*', r'astra\w*'],
    'SGZH': [r'сегежа\w*', r'segezha\w*'],
    'RNFT': [r'русснефт\w*', r'russneft\w*'],
    'MSNG': [r'мосэнерго\w*', r'mosenergo\w*'],
    'SMLT': [r'самолет\w*', r'samolet\w*'],
    'NVTK': [r'новатэк\w*', r'novatek\w*'],
    'ROSN': [r'роснефт\w*', r'rosneft\w*'],
    'TATN': [r'татнефт\w*', r'tatneft\w*'],
    'TATNP': [r'татнефт\w*', r'tatneft\w*'],
    'ETLN': [r'эталон\w*', r'etalon\w*'],
    'PRMD': [r'промомед\w*', r'promomed\w*'],
    'HNFG': [r'хэндерсон\w*', r'henderson\w*'],
    'AKRN': [r'акрон\w*', r'akron\w*'],
    'APTK': [r'аптечн\w*\s*сет\w*\s*36\w*', r'apteka.36\w*'],
    'ABIO': [r'артген\w*', r'artgen\w*'],
    'WUSH': [r'вуш\s', r'wush\w*'],
    'OGKB': [r'огк\s*2\w*', r'ogk.2\w*'],
    'DATA': [r'аренадата\w*', r'arendata\w*'],
    'FESH': [r'дальневосточн\w*\s*морск\w*\s*пароходств\w*', r'fesco\w*'],
    'DIAS': [r'диасофт\w*', r'diasoft\w*'],
    'IVAT': [r'ива\s', r'ivat\w*'],
    'KMAZ': [r'камаз\w*', r'kamaz\w*'],
    'DELI': [r'каршеринг\s*руссия\w*', r'delicar\w*'],
    'OZPH': [r'озон\s*фармацевтик\w*', r'ozon.pharma\w*'],
    'RASP': [r'распадская\w*', r'raspadskaya\w*'],
    'MRKV': [r'россети\s*волга\w*', r'rosseti.volga\w*'],
    'MSRS': [r'россети\s*московск\w*', r'rosseti.moscow\w*'],
    'MRKZ': [r'россети\s*северо-запад\w*', r'rosseti.northwest\w*'],
    'MRKU': [r'россети\s*урал\w*', r'rosseti.ural\w*'],
    'MRKP': [r'россети\s*центр\s*приволж\w*', r'rosseti.center.privolzhye\w*'],
    'MRKC': [r'россети\s*центр\w*', r'rosseti.center\w*'],
    'SVAV': [r'соллерс\w*', r'sollers\w*'],
    'SOFL': [r'софтлайн\w*', r'softline\w*'],
    'SNGS': [r'сургутнефтегаз\w*', r'surgutneftegas\w*'],
    'SNGSP': [r'сургутнефтегаз\w*', r'surgutneftegas\w*'],
    'TGKN': [r'тгк\s*14\w*', r'tgk.14\w*'],
    'TRMK': [r'тмк\w*', r'tmk\w*', r'трубн\w*\s*металлургическ\w*'],
    'UGLD': [r'южуралзолото\w*', r'uzg\w*'],
    'VSMO': [r'всмпо-ависма\w*', r'vsmpo.avisma\w*'],
    'RDRB': [r'рдрб\w*', r'russian.road.bank\w*'],
    'AVAN': [r'авангард\w*', r'avangard\w*'],
    'KZOS': [r'органическ\w*\s*синтез\w*', r'kzos\w*'],
    'KZOSP': [r'органическ\w*\s*синтез\w*', r'kzos\w*'],
    'LNZL': [r'лензолото\w*', r'lenzoloto\w*'],
    'LNZLP': [r'лензолото\w*', r'lenzoloto\w*'],
    'OZON': [r'озон\w*', r'ozon\w*'],
    'BLNG': [r'белон\w*', r'belon\w*'],
    'DZRD': [r'донской\s*завод\w*', r'donskoy.zavod\w*'],
    'DZRDP': [r'донской\s*завод\w*', r'donskoy.zavod\w*'],
    'MGNZ': [r'соликамск\w*\s*магниев\w*', r'solikamsk.magnesium\w*'],
    'BSPBP': [r'банк\s*санкт-петербург\w*', r'bank.st.petersburg\w*'],
    'NKNC': [r'нижнекамскнефтехим\w*', r'nizhnekamskneftekhim\w*'],
    'NKNCP': [r'нижнекамскнефтехим\w*', r'nizhnekamskneftekhim\w*'],
    'GEMA': [r'международн\w*\s*медицинск\w*\s*центр\w*', r'gemc\w*'],
    'KLVZ': [r'кристалл\w*', r'kristall\w*'],
    'APRI': [r'апри\w*', r'apri\w*'],
    'ABRD': [r'абрау\s*дюрсо\w*', r'abrau.durso\w*'],
    'UTAR': [r'ютэйр\w*', r'utair\w*'],
    'BANE': [r'башнефт\w*', r'bashneft\w*'],
    'BANEP': [r'башнефт\w*', r'bashneft\w*'],
    'ASSB': [r'астраханск\w*\s*энергосбыт\w*', r'astrakhan.energosbyt\w*'],
    'AMEZ': [r'ашинск\w*\s*металлургическ\w*', r'ashinsky.metallurgical\w*'],
    'USBN': [r'уралсиб\w*', r'uralsib\w*'],
    'BISVP': [r'башинформсвяз\w*', r'bashinformsvyaz\w*'],
    'BRZL': [r'бурятзолото\w*', r'buryatzoloto\w*'],
    'VLHZ': [r'владимирск\w*\s*химическ\w*', r'vladimir.chemical\w*'],
    'VGSB': [r'волгоградэнергосбыт\w*', r'volgogradenergosbyt\w*'],
    'VGSBP': [r'волгоградэнергосбыт\w*', r'volgogradenergosbyt\w*'],
    'VSYD': [r'выборгск\w*\s*судостроительн\w*', r'vyborg.shipyard\w*'],
    'VSYDP': [r'выборгск\w*\s*судостроительн\w*', r'vyborg.shipyard\w*'],
    'GAZT': [r'газ-тек\w*', r'gaz.tek\w*'],
    'GAZS': [r'газ-сервис\w*', r'gaz.service\w*'],
    'GAZC': [r'газкон\w*', r'gazcon\w*'],
    'GTRK': [r'глобалтрак\w*', r'globaltruck\w*'],
    'RTGZ': [r'газпром\s*газораспределение\w*', r'gazprom.gazoraspredelenie\w*'],
    'SIBN': [r'газпром\s*нефт\w*', r'gazprom.neft\w*'],
    'GCHE': [r'черкизово\w*', r'cherkizovo\w*'],
    'RBCM': [r'рбк\w*', r'rbk\w*'],
    'DVEC': [r'дальневосточн\w*\s*энергетическ\w*', r'far.east.energy\w*'],
    'EELT': [r'европейск\w*\s*электротехник\w*', r'european.electrotechnics\w*'],
    'ZVEZ': [r'звезда\w*', r'zvezda\w*'],
    'ZILL': [r'зил\s', r'zil\w*'],
    'RUSI': [r'русс-инвест\w*', r'russ.invest\w*'],
    'IGST': [r'ижсталь\w*', r'izhstal\w*'],
    'IGSTP': [r'ижсталь\w*', r'izhstal\w*'],
    'KLSB': [r'калужск\w*\s*сбыт\w*', r'kaluga.sbyt\w*'],
    'KOGK': [r'коршуновск\w*', r'korshunovsky\w*'],
    'KRSB': [r'красноярскэнергосбыт\w*', r'krasnoyarskenergosbyt\w*'],
    'KRSBP': [r'красноярскэнергосбыт\w*', r'krasnoyarskenergosbyt\w*'],
    'KAZT': [r'куйбышевазот\w*', r'kuibyshevazot\w*'],
    'KAZTP': [r'куйбышевазот\w*', r'kuibyshevazot\w*'],
    'KGKC': [r'курганск\w*\s*генериру\w*', r'kurgan.generating\w*'],
    'KGKCP': [r'курганск\w*\s*генериру\w*', r'kurgan.generating\w*'],
    'LMBZ': [r'ламбумиз\w*', r'lambumiz\w*'],
    'LVHK': [r'левенгук\w*', r'levenguk\w*'],
    'LPSB': [r'липецк\w*\s*энергосбыт\w*', r'lipetsk.energosbyt\w*'],
    'MGKL': [r'мгкл\w*', r'mgkl\w*'],
    'MSTT': [r'мостотрест\w*', r'mostotrest\w*'],
    'MGNT': [r'магнит\s', r'magnit\w*'],
    'MRSB': [r'мордовск\w*\s*энергосбыт\w*', r'mordovia.energosbyt\w*'],
    'MGTS': [r'московск\w*\s*телефон\w*', r'moscow.telephone\w*'],
    'MGTSP': [r'московск\w*\s*телефон\w*', r'moscow.telephone\w*'],
    'KROT': [r'красный\s*октябрь\w*', r'krasny.oktyabr\w*'],
    'KROTP': [r'красный\s*октябрь\w*', r'krasny.oktyabr\w*'],
    'NFAZ': [r'нефаз\w*', r'nefaz\w*'],
    'NSVZ': [r'наука-связ\w*', r'nauka.svyaz\w*'],
    'UWGN': [r'объединенная\s*вагонная\w*', r'united.wagon\w*'],
    'NKSH': [r'нижнекамскшина\w*', r'nizhnekamskshina\w*'],
    'NKHP': [r'новороссийск\w*\s*хлебопродукт\w*', r'novorossiysk.khleboprodukt\w*'],
    'NMTP': [r'новороссийск\w*\s*морск\w*\s*порт\w*', r'novorossiysk.port\w*'],
    'UNAC': [r'объединенная\s*авиастроительн\w*', r'united.aircraft\w*'],
    'PAZA': [r'павловск\w*\s*автобус\w*', r'pavlovsky.bus\w*'],
    'PMSB': [r'пермск\w*\s*энергосбыт\w*', r'perm.energosbyt\w*'],
    'PMSBP': [r'пермск\w*\s*энергосбыт\w*', r'perm.energosbyt\w*'],
    'CHGZ': [r'рн-западная\w*', r'rn.western\w*'],
    'ROST': [r'росинтер\w*', r'rosinter\w*'],
    'RKKE': [r'энергия\w*', r'energy\w*', r'ракетно-космическ\w*'],
    'LSNG': [r'россети\s*ленэнерго\w*', r'rosseti.lenenergo\w*'],
    'LSNGP': [r'россети\s*ленэнерго\w*', r'rosseti.lenenergo\w*'],
    'MRKK': [r'россети\s*северный\s*кавказ\w*', r'rosseti.north.caucasus\w*'],
    'MRKS': [r'россети\s*сибирь\w*', r'rosseti.siberia\w*'],
    'TORS': [r'россети\s*томск\w*', r'rosseti.tomsk\w*'],
    'TORSP': [r'россети\s*томск\w*', r'rosseti.tomsk\w*'],
    'MRKY': [r'россети\s*юг\w*', r'rosseti.south\w*'],
    'ROLO': [r'русолово\w*', r'rusolovo\w*'],
    'RZSB': [r'рязанск\w*\s*энергосбыт\w*', r'ryazan.energosbyt\w*'],
    'SPBE': [r'спб\s*бирж\w*', r'spb.exchange\w*'],
    'KRKN': [r'саратовск\w*\s*нефтеперерабатыва\w*', r'saratov.refinery\w*'],
    'KRKNP': [r'саратовск\w*\s*нефтеперерабатыва\w*', r'saratov.refinery\w*'],
    'SARE': [r'саратовэнерго\w*', r'saratovenergo\w*'],
    'SAREP': [r'саратовэнерго\w*', r'saratovenergo\w*'],
    'SVET': [r'светофор\w*', r'svetofor\w*'],
    'SVETP': [r'светофор\w*', r'svetofor\w*'],
    'MFGS': [r'славнефт\w*мегионнефтегаз\w*', r'slavneft.megionneftegaz\w*'],
    'MFGSP': [r'славнефт\w*мегионнефтегаз\w*', r'slavneft.megionneftegaz\w*'],
    'JNOS': [r'славнефт\w*ярославнефтеоргсинтез\w*', r'slavneft.yaroslavnefteorgsintez\w*'],
    'JNOSP': [r'славнефт\w*ярославнефтеоргсинтез\w*', r'slavneft.yaroslavnefteorgsintez\w*'],
    'CARM': [r'смарттехгрупп\w*', r'smarttechgroup\w*'],
    'STSB': [r'ставропольэнергосбыт\w*', r'stavropol.energosbyt\w*'],
    'STSBP': [r'ставропольэнергосбыт\w*', r'stavropol.energosbyt\w*'],
    'PRFN': [r'теплант\w*', r'teplant\w*'],
    'VRSB': [r'тнс\s*энерго\s*воронеж\w*', r'tns.energo.voronezh\w*'],
    'VRSBP': [r'тнс\s*энерго\s*воронеж\w*', r'tns.energo.voronezh\w*'],
    'KBSB': [r'тнс\s*энерго\s*кубань\w*', r'tns.energo.kuban\w*'],
    'MISB': [r'тнс\s*энерго\s*марий\s*эл\w*', r'tns.energo.mari.el\w*'],
    'MISBP': [r'тнс\s*энерго\s*марий\s*эл\w*', r'tns.energo.mari.el\w*'],
    'NNSB': [r'тнс\s*энерго\s*нижний\s*новгород\w*', r'tns.energo.nizhny.novgorod\w*'],
    'NNSBP': [r'тнс\s*энерго\s*нижний\s*новгород\w*', r'tns.energo.nizhny.novgorod\w*'],
    'RTSB': [r'тнс\s*энерго\s*ростов\w*', r'tns.energo.rostov\w*'],
    'RTSBP': [r'тнс\s*энерго\s*ростов\w*', r'tns.energo.rostov\w*'],
    'YRSB': [r'тнс\s*энерго\s*ярославль\w*', r'tns.energo.yaroslavl\w*'],
    'YRSBP': [r'тнс\s*энерго\s*ярославль\w*', r'tns.energo.yaroslavl\w*'],
    'TASB': [r'тамбовск\w*\s*энергосбыт\w*', r'tambov.energosbyt\w*'],
    'TASBP': [r'тамбовск\w*\s*энергосбыт\w*', r'tambov.energosbyt\w*'],
    'TTLK': [r'таттелеком\w*', r'tattelecom\w*'],
    'TGKB': [r'тгк\s*2\w*', r'tgk.2\w*'],
    'TGKBP': [r'тгк\s*2\w*', r'tgk.2\w*'],
    'TUZA': [r'туймазинск\w*\s*автобетоновоз\w*', r'tuymazinsky.autobetonovoz\w*'],
    'UKUZ': [r'южный\s*кузбасс\w*', r'southern.kuzbass\w*'],
    'URKZ': [r'уральская\s*кузница\w*', r'uralskaya.kuznitsa\w*'],
    'LIFE': [r'фармсинтез\w*', r'pharmsintez\w*'],
    'CNTL': [r'центральный\s*телеграф\w*', r'central.telegraph\w*'],
    'CNTLP': [r'центральный\s*телеграф\w*', r'central.telegraph\w*'],
    'CHKZ': [r'челябинск\w*\s*кузнечно-прессов\w*', r'chelyabinsk.forge\w*'],
    'CHMK': [r'челябинск\w*\s*металлургическ\w*', r'chelyabinsk.metallurgical\w*'],
    'ELMT': [r'элемент\w*', r'element\w*'],
    'UNKL': [r'южно-уральск\w*\s*никел\w*', r'south.urals.nickel\w*'],
    'IRKT': [r'яковлев\w*', r'yakovlev\w*'],
    'YAKG': [r'якутск\w*\s*топливно-энергетическ\w*', r'yakutsk.fuel.energy\w*'],
    'YKEN': [r'якутскэнерго\w*', r'yakutskenergo\w*'],
    'YKENP': [r'якутскэнерго\w*', r'yakutskenergo\w*'],
    'KUZB': [r'кузнецк\w*', r'kuznetsk\w*'],
    'TNSE': [r'тнс\s*энерго\w*', r'tns.energo\w*'],
    'DIOD': [r'диод\w*', r'diod\w*'],
    'ZAYM': [r'займер\w*', r'zaimer\w*'],
    'NAUK': [r'наука\w*', r'nauka\w*'],
    'RGSS': [r'росгосстрах\w*', r'rosgosstrakh\w*'],
    'KCHE': [r'камчатскэнерго\w*', r'kamchatskenergo\w*'],
    'KCHEP': [r'камчатскэнерго\w*', r'kamchatskenergo\w*'],
    'MAGE': [r'магаданэнерго\w*', r'magadanenergo\w*'],
    'MAGEP': [r'магаданэнерго\w*', r'magadanenergo\w*'],
    'SAGO': [r'самараэнерго\w*', r'samaraenergo\w*'],
    'SAGOP': [r'самараэнерго\w*', r'samaraenergo\w*'],
    'SLEN': [r'сахалинэнерго\w*', r'sakhalinenergo\w*'],
    'PRMB': [r'приморье\w*', r'primorye\w*'],
    'KMEZ': [r'ковровск\w*\s*механическ\w*', r'kovrov.mechanical\w*'],
    'ARSA': [r'арсагера\w*', r'arsagera\w*'],
    'VEON-RX': [r'veon\w*'],
    'HIMCP': [r'химпром\w*', r'khimprom\w*'],
    'GECO': [r'генетико\w*', r'genetico\w*'],
    'WTCM': [r'центр\s*международной\s*торговли\w*', r'world.trade.center\w*'],
    'WTCMP': [r'центр\s*международной\s*торговли\w*', r'world.trade.center\w*'],
    'OMZZP': [r'уралмаш-ижора\w*', r'uralmash.izhora\w*'],
    'VJGZ': [r'варьеганнефтегаз\w*', r'varyeganneftegaz\w*'],
    'VJGZP': [r'варьеганнефтегаз\w*', r'varyeganneftegaz\w*'],
    'KRKOP': [r'красный\s*котельщик\w*', r'krasny.kotelshchik\w*'],
    'KFBA': [r'ингард\w*', r'ingrad\w*']
}



positive_keywords = [
    r'рост\w*', r'увеличение\w*', r'прибыль\w*', r'улучшение\w*', r'поддержка\w*', 
    r'субсиди\w*', r'льгот\w*', r'помощь\w*', r'успешн\w*\s*запуск\w*', 
    r'ввод\s*в\s*эксплуатацию\w*', r'потенциал\s*рост\w*', r'выгодн\w*\s*вложение\w*', 
    r'выгодн\w*', r'восстановление\w*', r'возобновление\w*', r'снижение\s*налог\w*', 
    r'налогов\w*\s*каникул\w*', r'одобрение\w*', r'разрешение\w*', 
    r'бесперебойн\w*\s*работ\w*', r'надёжн\w*', r'цифров\w*\s*трансформац\w*', 
    r'развитие\s*технологи\w*', r'смягчение\s*мер\w*', r'отмена\s*ограничений\w*', 
    r'позитивн\w*', r'оптимистичн\w*', r'рекордн\w*', r'увеличил\w*', r'выросл\w*', 
    r'превысил\w*', r'улучшил\w*', r'достигнут\w*', r'прорыв\w*', r'инноваци\w*', 
    r'модернизаци\w*', r'эффективность\w*', r'доходность\w*', r'дивиденд\w*', 
    r'преми\w*', r'бонус\w*', r'стимулирование\w*', r'расширение\w*', r'инвестици\w*', 
    r'развитие\w*', r'процветание\w*', r'успех\w*'
]

negative_keywords = [
    r'падение\w*', r'снижение\w*', r'убыток\w*', r'спад\w*', r'санкци\w*', 
    r'ограничение\w*', r'давление\w*', r'пандеми\w*', r'коронавирус\w*', 
    r'карантин\w*', r'обвал\w*', r'распродаж\w*', r'волатильность\w*', 
    r'сокращение\w*', r'закрытие\w*', r'приостановк\w*', r'риск\w*', r'угроз\w*', 
    r'ухудшение\w*', r'прогноз\w*', r'авария\w*', r'разлив\w*', r'расследование\w*', 
    r'задолженность\w*', r'дефицит\w*', r'увольнение\w*', r'протест\w*', 
    r'нестабильность\w*', r'сложност\w*', r'проблем\w*', r'напряжённость\w*', 
    r'конфликт\w*', r'кризис\w*', r'сложн\w*\s*ситуац\w*', r'вызов\w*', 
    r'негативн\w*', r'пессимистичн\w*', r'снизил\w*', r'упал\w*', r'ухудшил\w*', 
    r'трудност\w*', r'потер\w*', r'ущерб\w*', r'банкротство\w*', r'рецесси\w*', 
    r'стагнаци\w*', r'инфляци\w*', r'девальваци\w*', r'обесценение\w*', 
    r'сократил\w*', r'приостановил\w*', r'штраф\w*', r'иск\w*', r'суд\w*', 
    r'забастовк\w*', r'замедление\w*'
]

neutral_keywords = [
    r'планируется\w*', r'рассматривается\w*', r'ожидается\w*', r'данн\w*', 
    r'отчёт\w*', r'результат\w*', r'заявление\w*', r'сообщение\w*', 
    r'информирование\w*', r'встреч\w*', r'переговор\w*', r'совещание\w*', 
    r'прогноз\w*', r'оценк\w*', r'анализ\w*', r'изменение\w*', r'поправк\w*', 
    r'решение\w*', r'мониторинг\w*', r'обсуждение\w*', r'публикация\w*', 
    r'информация\w*', r'сообщает\w*', r'отмечает\w*', r'заявил\w*', r'сообщил\w*', 
    r'подписан\w*', r'соглашение\w*', r'договор\w*', r'контракт\w*', 
    r'рекомендация\w*', r'совет\w*', r'заседание\w*', r'мероприятие\w*', 
    r'презентация\w*', r'конференция\w*', r'форум\w*'
]


topic_keywords = {
    'МАКРОЭКОНОМИКА': [
        r'ключев[а-я]+\s+ставк[а-я]+',
        r'инфляц[а-я]+',
        r'ввп',
        r'бюджет',
        r'налог[а-я]*',
        r'ндпи',
        r'курс\s+рубл[а-я]+',
        r'золотовалютн[а-я]+\s+резерв[а-я]*',
        r'минфин',
        r'казначейств[а-я]*',
        r'госдолг'
    ],
    
    'КОРПОРАТИВНЫЕ_НОВОСТИ': [
        r'чист[а-я]+\s+прибыль',
        r'выручк[а-я]+',
        r'дивиденд[а-я]*',
        r'совет\s+директор[а-я]*',
        r'капитальн[а-я]+\s+затрат[а-я]*',
        r'облигац[а-я]*',
        r'кредитн[а-я]+\s+рейтинг',
        r'ebitda',
        r'рсбу',
        r'мсфо',
        r'инвестпрограмм[а-я]*'
    ],
    
    'ЭНЕРГЕТИКА': [
        r'северн[а-я]+\s+поток',
        r'турецк[а-я]+\s+поток',
        r'газопровод[а-я]*',
        r'добыч[а-я]+\s+нефт[а-я]*',
        r'опек\+',
        r'спг',
        r'газпром',
        r'нефтегаз',
        r'трубоукладчик',
        r'экспорт\s+газ[а-я]*'
    ],
    
    'ТРАНСПОРТ': [
        r'авиасообщен[а-я]*',
        r'авиакомпани[а-я]*',
        r'пассажиропоток',
        r'росавиац[а-я]*',
        r'субсиди[а-я]+\s+авиаперевоз[а-я]*',
        r'аэропорт[а-я]*',
        r'перевозк[а-я]+\s+пассажир[а-я]*',
        r'рейс[а-я]*'
    ],
    
    'САНКЦИИ': [
        r'санкц[а-я]*',
        r'caatsa',
        r'госдеп\s+сша',
        r'\sес\s',
        r'мид',
        r'дипломати[а-я]*',
        r'геополитик[а-я]*',
        r'ответн[а-я]+\s+мер[а-я]*'
    ],
    
    'COVID19': [
        r'коронавирус[а-я]*',
        r'ковид',
        r'пандеми[а-я]*',
        r'карантин',
        r'ограничен[а-я]*',
        r'вакцин[а-я]*',
        r'заболеваемост[а-я]*',
        r'антиковидн[а-я]*',
        r'удаленк[а-я]*'
    ],
    
    'ЧП_АВАРИИ': [
        r'авари[а-я]*',
        r'чрезвычайн[а-я]+\s+происшестви[а-я]*',
        r'разлив',
        r'ростехнадзор',
        r'расследован[а-я]*',
        r'экологическ[а-я]+\s+ущерб',
        r'тэц',
        r'нефтеразлив'
    ],
    
    'ТЕХНОЛОГИИ': [
        r'цифровизац[а-я]*',
        r'it[-\s]*компани[а-я]*',
        r'телеком[а-я]*',
        r'5g',
        r'минцифр[а-я]*',
        r'софт',
        r'гаджет[а-я]*',
        r'кибербезопасност[а-я]*'
    ],
    
    'ФОНДОВЫЙ_РЫНОК': [
        r'фондов[а-я]+\s+рын[а-я]*',
        r'индекс[а-я]*\s+ммвб',
        r'индекс[а-я]*\s+ртс',
        r'котировк[а-я]*',
        r'нефть\s+brent',
        r'волатильност[а-я]*',
        r'инвестор[а-я]*',
        r'аналитик[а-я]*',
        r'целев[а-я]+\s+цен[а-я]*'
    ],
    
    'ЖКХ': [
        r'тариф[а-я]*\s+жкх',
        r'теплоснабжен[а-я]*',
        r'моэк',
        r'поддержк[а-я]+\s+регион[а-я]*',
        r'социальн[а-я]+\s+выплат[а-я]*',
        r'пенси[а-я]*',
        r'мрот',
        r'детск[а-я]+\s+пособи[а-я]*'
    ],
    
    'БАНКИ': [
        r'прибыль\s+банк[а-я]*',
        r'кредитн[а-я]+\s+портфель',
        r'просрочк[а-я]*',
        r'норматив[а-я]*',
        r'цб\s+рф',
        r'набиуллин[а-я]*',
        r'ликвидност[а-я]*',
        r'резерв[а-я]*'
    ],
    
    'ПОЛИТИКА': [
        r'выбор[а-я]*',
        r'госдум[а-я]*',
        r'совет\s+федерац[а-я]*',
        r'парти[а-я]*',
        r'предвыборн[а-я]+\s+кампани[а-я]*',
        r'инаугурац[а-я]*',
        r'политсовет'
    ]
}





def determine_trading_day_impact(publish_date):
    """
    Определяет, на какой торговый день повлияет новость:
    1 - текущий торговый день (новость вышла с 00:00:00 до открытия торгов)
    2 - следующий торговый день (новость вышла после закрытия торгов и до 00:00:00)
    """
    try:
        if isinstance(publish_date, str):
            dt = datetime.strptime(publish_date, '%Y-%m-%d %H:%M:%S')
        else:
            return 2  
        

        time_obj = dt.time()
        
    
        market_open = time(9, 50, 0)   
        market_close = time(18, 50, 0) 

        if time(0, 0, 0) <= time_obj < market_open:
            return 1 
        

        elif market_close < time_obj <= time(23, 59, 59):
            return 2  
        

        elif market_open <= time_obj <= market_close:
            return 1  
            
    except Exception as e:
        print(f"Ошибка при обработке даты {publish_date}: {e}")
        return 2  
    



def count_words(text):
    if not isinstance(text, str):
        return 0
    # Удаляем лишние пробелы и разбиваем текст на слова
    words = text.strip().split()
    return len(words)



def find_tickers(text):
    found_tickers = []
    if not isinstance(text, str):
        return found_tickers
        
    text_lower = text.lower()
    
    for ticker, patterns in ticker_keywords.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                found_tickers.append(ticker)
                break  # Чтобы избежать дублирования тикера
    
    return list(set(found_tickers))



def count_tickers(tickers_list):
    if not isinstance(tickers_list, list):
        return 0
    return len(tickers_list)


def analyze_sentiment(text):
    if not isinstance(text, str):
        return 0
    
    text_lower = text.lower()
    
    # Подсчет ключевых слов с использованием регулярных выражений
    positive_count = sum(1 for pattern in positive_keywords if re.search(pattern, text_lower))
    negative_count = sum(1 for pattern in negative_keywords if re.search(pattern, text_lower))
    neutral_count = sum(1 for pattern in neutral_keywords if re.search(pattern, text_lower))
    
    # Определение тональности на основе преобладающих ключевых слов
    if positive_count > negative_count and positive_count > 0:
        return 1  # Позитивная
    elif negative_count > positive_count and negative_count > 0:
        return -1  # Негативная
    else:
        return 0  # Нейтральная
    


def analyze_topic(text):
    if not isinstance(text, str):
        return []
    
    text_lower = text.lower()
    found_topics = []
    
    for topic, patterns in topic_keywords.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                found_topics.append(topic)
                break  # Если нашли хотя бы одно совпадение для темы, переходим к следующей теме
    
    return found_topics



def check_keywords(text):
    """
    Проверяет наличие ключевых слов в тексте и возвращает бинарные признаки
    """
    if not isinstance(text, str):
        return {
            'sanctions': 0,
            'central_bank': 0, 
            'war': 0,
            'dividends': 0
        }
    
    text_lower = text.lower()
    
    # Проверяем наличие слова "санкции" (учитываем разные формы)
    sanctions_pattern = r'\bсанкц(и[ий]|ионн|иями?|иям?|иях?)?\b'
    sanctions = 1 if re.search(sanctions_pattern, text_lower) else 0
    
    # Проверяем наличие упоминаний ЦБ (Центральный Банк)
    cb_pattern = r'\b(ц[б]|центробанк|центральный\s+банк|банк\s+росси[и])\b'
    central_bank = 1 if re.search(cb_pattern, text_lower) else 0
    
    # Проверяем наличие слова "война" (учитываем разные формы)
    war_pattern = r'\bво[йе]н(н?ы[йм]?|а|у|о[й]|е|ам[и]?|ах?|)?\b'
    war = 1 if re.search(war_pattern, text_lower) else 0
    
    # Проверяем наличие слова "дивиденды" (учитываем разные формы)
    dividends_pattern = r'\bдивидент?н?(ды|дов|дам[и]?|дах?|дами?|дн)?\b'
    dividends = 1 if re.search(dividends_pattern, text_lower) else 0
    
    return {
        'sanctions': sanctions,
        'central_bank': central_bank,
        'war': war,
        'dividends': dividends
    }




def feature_text_generate(df):
    df['combined_text'] = df['title'].fillna('') + ' ' + df['publication'].fillna('')
    df['tickers'] = df['combined_text'].apply(find_tickers)
    df['sentiment'] = df['combined_text'].apply(analyze_sentiment)
    df['topics'] = df['combined_text'].apply(analyze_topic)
    """df['topics_str'] = df['topics'].apply(lambda x: ', '.join(x) if x else 'НЕ_ОПРЕДЕЛЕНО')"""
    df['word_count'] = df['combined_text'].apply(count_words)
    df['ticker_count'] = df['tickers'].apply(count_tickers)
    df['trading_day_impact'] = df['publish_date'].apply(determine_trading_day_impact)
    keyword_results = df['combined_text'].apply(check_keywords)

    # Добавляем новые столбцы в DataFrame
    df['sanctions'] = keyword_results.apply(lambda x: x['sanctions'])
    df['central_bank'] = keyword_results.apply(lambda x: x['central_bank'])
    df['war'] = keyword_results.apply(lambda x: x['war'])
    df['dividends'] = keyword_results.apply(lambda x: x['dividends'])
    df.drop('combined_text', axis=1, inplace=True)
    # Размножаем строки по тикерам
    df_expanded = df.explode('tickers')

    # Обновляем структуру данных
    df_expanded = df_expanded.reset_index(drop=True)
    df_expanded['tickers'] = df_expanded['tickers'].apply(lambda x: [x] if pd.notna(x) else [])
    df_expanded['ticker_count'] = 1

    # Заменяем исходный DataFrame
    df = df_expanded

    # Преобразуем столбец в datetime и обнуляем время
    df['publish_date'] = pd.to_datetime(df['publish_date']).dt.normalize()

    df['ticker'] = df['tickers'].apply(lambda x: x[0] if len(x) > 0 else None)

    # Теперь сгруппируем по дате и тикеру
    grouped = df.groupby(['publish_date', 'ticker']).agg({
        'sentiment': ['mean', 'sum', 'count']  # среднее, сумма и количество
    }).reset_index()


    # Группируем по дате и тикеру с учетом новых признаков
    grouped_with_keywords = df.groupby(['publish_date', 'ticker']).agg({
        'sentiment': ['mean', 'sum', 'count'],
        'word_count': ['mean', 'sum'],
        'sanctions': 'sum',  # количество новостей с санкциями
        'central_bank': 'sum',  # количество новостей с ЦБ
        'war': 'sum',  # количество новостей с войной
        'dividends': 'sum',  # количество новостей с дивидендами
        'ticker_count': 'count'
    }).reset_index()

    # Переименуем столбцы
    grouped_with_keywords.columns = [
        'publish_date', 'ticker', 
        'sentiment_mean', 'sentiment_sum', 'sentiment_count',
        'word_count_mean', 'word_count_sum',
        'sanctions_count', 'central_bank_count', 'war_count', 'dividends_count',
        'news_count'
    ]

    # Добавим доли (проценты) для каждого признака
    grouped_with_keywords['sanctions_ratio'] = grouped_with_keywords['sanctions_count'] / grouped_with_keywords['news_count']
    grouped_with_keywords['central_bank_ratio'] = grouped_with_keywords['central_bank_count'] / grouped_with_keywords['news_count']
    grouped_with_keywords['war_ratio'] = grouped_with_keywords['war_count'] / grouped_with_keywords['news_count']
    grouped_with_keywords['dividends_ratio'] = grouped_with_keywords['dividends_count'] / grouped_with_keywords['news_count']


    return grouped_with_keywords
