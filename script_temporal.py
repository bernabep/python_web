from datetime import datetime,time,timedelta
from dateutil.relativedelta import relativedelta
def calcular_dias_acumular_por_meses():    
    hoy = datetime.today()
    primer_dia_mes_actual = datetime(hoy.year,hoy.month,1)
    ultimo_dia_mes_pasado = primer_dia_mes_actual-timedelta(days=1)
    num_dias_desde_mes_pasados = (ultimo_dia_mes_pasado.day + hoy.day)-1
    return num_dias_desde_mes_pasados

print (calcular_dias_acumular_por_meses())