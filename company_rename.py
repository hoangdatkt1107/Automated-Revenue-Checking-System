def rename_company(value):
    if value in ['BSM']:
        return 'BSM'
    elif value in ['LETSGO']:
        return 'LETSGO'
    elif value in ['TAMDUC_HANOI', 'TAMDUC','TAMDUC_DONGXOAI']:
        return 'TAMDUC'
    elif value in ['NGUYENKIM','CRVHO','CRV_SPORT','HYPERMARKET','CRV_CMG','PROPERTY','CRV']:
        return 'CRV'
    elif value in ['AGM','AGM_MB','MLR','RWH']:
        return 'ANNAM'
    elif value in ['MUTOSI']:
        return 'MUTOSI'
    elif value in ['KGRVP','KGRHCM']:
        return 'KANGAROO'
    elif value in ['NHUADONGNAI']:
        return 'NHUADONGNAI'
    elif value in ['GRGR']:
        return 'GRGR'
    elif value in ['SEAZEN']:
        return 'SEAZEN'
    elif value in ['GRGR']:
        return 'GRGR'
    elif value in ['BPG','BTD','BTP','BTL']:
        return 'BPG'
    elif value in ['DONGHUNG']:
        return 'DONGHUNG'
    elif value in ['TDP','TDECO','TDHY']:
        return 'TDP'
    elif value in ['SHYNHHOUSE','SHYNHGROUP','SHYNHBEAUTY']:
        return 'SHYNHGROUP'
    elif value in ['LC5200','LC5300','LC5500']:
        return 'LANCHI'
    elif value in ['FMSTORE']:
        return 'FAMIMART'
    elif value in ['HASECA','HASECA_MEKONG','HASECAMEKONG']:
        return 'HASECA'
    elif value in ['GDT']:
        return 'GODUCTHANH'
    elif value in ['TTF']:
        return 'TTF'
    elif value in ['FRT']:
        return 'FRT'
    elif value in ['FRTLC']:
        return 'FRTLC'
    elif value in ['SANHA02','SANHA01']:
        return 'SANHA'
    elif value in ['AGR']:
        return 'AGR'
    elif value in ['QTNP']:
        return 'QTNP'
    elif value in ['LCF','LCFKMSVINA','LCFJJ']:
        return 'LCF'
    elif value in ['VNR']:
        return 'VNR'
    elif value in ['POWERCONNECT']:
        return 'POWERCONNECT'
    elif value in ['MAYTAYSON']:
        return 'MAYTAYSON'
    elif value in ['LVF']:
        return 'LVF'
    elif value in ['XLMANPOWER']:
        return 'XLMANPOWER'
    elif value in ['NHUATANPHU']:
        return 'NHUATANPHU'
    elif value in ['TEKCOM']:
        return 'TEKCOM'
    elif value in ['GREENLEAF']:
        return 'GREENLEAF'
    elif value in ['HUMANPOWER']:
        return 'HUMANPOWER'
    elif value in ['TWEDUDT','TWEDUDQ','TWEDUHQ','TWEDUCL']:
        return 'TWEDU'
    elif value in ['MAYSONGTIEN']:
        return 'MAYSONGTIEN'
    elif value in ['MTP']:
        return 'MTP'
    elif value in ['CAMIMEX']:
        return 'CAMIMEX'
    elif value in ['HIEUBAC','VIETTRUNG','HIEUBACBINHDUONG']:
        return 'HIEUBAC'
    elif value in ['YODY','YGG','YOKIDS']:
        return 'YODY'
    elif value in ['VITC','LPTEX']:
        return 'DETMAYLIENPHUONG'
    elif value in ['NANO']:
        return 'NANO'
    elif value in ['GREENSPEEDGS21','GREENSPEEDHGX21','GREENSPEEDHN21','GREENSPEEDGS26','GREENSPEEDGS01','GREENSPEEDHN01','GREENSPEEDHGX01','GREENSPEED_GS','GREENSPEED_HN']:
        return 'GREENSPEED'
    elif value in ['LEASSOCIATES21','LEASSOCIATES22','LEASSOCIATES25','LEASSOCIATES01','LEASSOCIATESDT','KBM']:
        return 'LEASSOCIATES'
    elif value in ['HSV24','HOANVU24','HOANVU01','HSV01']:
        return 'HSV'
    elif value in ['TTV','TTVPROJECT']:
        return 'TTV'
    elif value in ['APD']:
        return 'APD'
    elif value in ['GS25']:
        return 'GS25'
    elif value in ['TLC']:
        return 'TLC'
    elif value in ['PHAMNGUYEN']:
        return 'PHAMNGUYEN'
    elif value in ['BBBD','DVNN','LTS','LTA','NMTH','NMVH','NMVB','VNC','NMTS','NMVL','LTG','LTL']:
        return 'LTG'
    elif value in ['DSAGO']:
        return 'DSAGO'
    else:
        return None
    
def rename_company_crv(value):
        if value in ['NGUYENKIM', 'CRVHO', 'CRV_SPORT', 'HYPERMARKET', 'CRV_CMG', 'PROPERTY','GO','CRV','TOPS','COMEHOME','BIPBIP']:
            return 'CRV'
        return value
    