import psutil
import warnings
import threading
import pandas as pd
import os
import shutil
import logging
import smtplib
import mysql.connector as mysql
from time import sleep
from copy import deepcopy
from datetime import time, timedelta, datetime
from playwright.sync_api import sync_playwright,Browser,expect
from email.message import EmailMessage
#Desactivar algunos warning en la terminal
warnings.filterwarnings("ignore", category=DeprecationWarning)
#Crea un Log de erroes
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("errores.log")
stream_handler = logging.StreamHandler()
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

#!ASIGNO VARIABLES GLOBALES
lista_minutos=[15,45]
lista_minutos=[15,33]
lista_colas = [
'VY SY Italiano',
'VY SY Frances',
'VY SY Ingles',
'VY SY Catalan',
'VY SY Espa��ol',
'VY S7 Visa It K',
'VY S7 Visa Fr K',
'VY S7 Visa Ca K',
'VY S7 Visa En K',
'VY S7 Visa Es K',
'VY S6 PAE It K',
'VY S6 PAE Fr K',
'VY S6 PAE Ca K',
'VY S6 PAE En K',
'VY S6 PAE Es K',
'VY S5 Reembolso It K',
'VY S5 Reembolso Fr K',
'VY S5 Reembolso En NDL K',
'VY S5 Reembolso En K',
'VY S5 Reembolso Es K',
'VY S4 Lost & Found En K',
'VY S4 Lost & Found Es K',
'VY S0 Disruptions Ca K',
'VY S0 Disruptions En K',
'VY S0 Disruptions Es K',
'VY S0 Disruptions Fr K',
'VY S0 Disruptions It K',
'VY S1 Grupos Ca K',
'VY S1 Grupos En K',
'VY S1 Grupos Es K',
'VY S1 Grupos Fr K',
'VY S1 Grupos It K',
'VY S1 Profesional Ca K',
'VY S1 Profesional En K',
'VY S1 Profesional Es K',
'VY S1 Profesional Fr K',
'VY S1 Profesional It K',
'VY S2 DoHop En',
'VY S2 DoHop Es',
'VY S2 Particular Ca K',
'VY S2 Particular En K',
'VY S2 Particular Es K',
'VY S2 Particular Fr K',
'VY S2 Particular It K',
'VY S3 Ventas Ca K',
'VY S3 Ventas En K',
'VY S3 Ventas Es K',
'VY S3 Ventas Fr K',
'VY S3 Ventas It K',
'VY S3 Club It K',
'VY S3 Club Fr K',
'VY S3 Club En K',
'VY S3 Club Ca K',
'VY S3 Club Es K',
'VY S3 Premium It K',
'VY S3 Premium Fr K',
'S3 Premium En K',
'VY S3 Premium Ca K',
'VY S3 Premium Es K',
'VY Campa��a Emision K ',
'VY S5 Reembolso Ca K'
]
lista_informes_a_sacar=['colas_individual_tramos']
lista_informes_a_sacar=['colas','tramos','colas_tramos','actividad_por_agente','actividad_por_agente_cola','estados_por_agente','agentes','listado_llamadas','listado_acd','skills_agentes']
silencioso=True #!Para que no muestre tantos print, hay que poner True
segundos_de_espera = 3 #!Dependiendo del PC, los segundos de espera tienen que aumentarse, afecta sobretodo al navegar por la web
segundos_de_espera_descarga = 60 #!Dependiendo del PC, los segundos de espera tienen que aumentarse, afecta sobretodo al navegar por la web
segundos_de_espera_descarga_aumentado = 120 #!Dependiendo del PC, los segundos de espera tienen que aumentarse, afecta sobretodo al navegar por la web
num_dias_para_acumular_colas = 5 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_tramos = 5 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_colas_tramos = 1 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_colas_individual_tramos = 22 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_actividad_por_agente = 5 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_actividad_por_agente_cola = 5 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_estados_por_agente = 3 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_agentes = 5 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_listado_llamadas = 3 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
num_dias_para_acumular_listado_acd = 3 #!Importante, si se ponen menos días, se eliminarán del acumulado los días que no cumplen la condición
primer_tramo_del_dia = 8 #!Importante, necesario para saber si sacamos todos los tramos desde las 0 hrs o desde las 8 de la mañana por ejemplo
ruta_destino = f"C:\\Users\\berna\\Desktop\\MUESTRA" #!ruta de destino donde se almacenan los archivos definitivos
ruta_destino = f"\\\\bcnsmb01.grupokonecta.corp\\SERVICIOS\\BOLL_COMUN_ANALISTAS\\Importar\\Robot" #!ruta de destino donde se almacenan los archivos definitivos
ruta_destino = rf"C:\Users\bpandofer\Desktop\Robot" #!ruta de destino donde se almacenan los archivos definitivos
ruta_destino = rf"C:\Users\berna\Desktop\Masvoz" #!ruta de destino donde se almacenan los archivos definitivos

#?VARIABLES RELATIVAS A NAVICAT
host="172.15.9.179"
user='bpandof'
password='123456789'
database='bd_analistas_boll'
tabla_colas= 'vv_Informe_colas'
tabla_tramos= 'vv_Informe_tramos'
tabla_colas_tramos= 'vv_Informe_colas_tramos'
tabla_colas_indivual_tramos= 'vv_Informe_colas_individual_tramos'
tabla_actividad_por_agente = 'vv_Informe_actividad_por_agente'
tabla_estados_por_agente = 'vv_Informe_estados_por_agente'
tabla_agentes = 'vv_Informe_agentes'
tabla_listado_llamadas = 'vv_Informe_listado_llamadas'
tabla_listados_acd = 'vv_Informe_listado_acd'

#?VARIABLES RELATIVAS Al MAIL
# Configuracion del servidor SMTP
miCorreo= "bernabe.pando@konecta-group.com"
destinatarioCorreo= "bernabe.pando@konecta-group.com"
# miPass = "odix zpmr kvvy satr"
miPass = "BSPfBSPf008*"

url = 'https://manager.masvoz.es/'

# ---------------------------------------------------------------------------------------------------------------
# MATAR PROCESO WINDOWS POR NOMBRE
# ---------------------------------------------------------------------------------------------------------------
def matar_proceso(nombre_proceso):
    for proc in psutil.process_iter(["pid", "name"]):
        if nombre_proceso.lower() in proc.info["name"].lower():
            pid = proc.info["pid"]
            try:
                proceso = psutil.Process(pid)
                proceso.terminate()
                # print(f"Proceso {nombre_proceso} (PID {pid}) terminado.")
            # except psutil.NoSuchProcess as e:
                # print(f"El proceso {nombre_proceso} no existe: {e}")
            except psutil.AccessDenied as e:
                print(f"No se pudo terminar el proceso {nombre_proceso}: {e}")
            except Exception as e:
                print(
                    f"No se pudo terminar el proceso {nombre_proceso} con terminate(). Intentando con kill(): {e}"
                )
                try:
                    proceso.kill()
                    # print(f"Proceso {nombre_proceso} (PID {pid}) terminado con kill().")
                except Exception as e:
                    print(
                        f"No se pudo terminar el proceso {nombre_proceso} con kill(): {e}"
                    )

#para buscar por class usas .
#para buscar por id usasa #
def lanzar_playwright():
    return sync_playwright().start()
def abrir_navegador(oculto=False):   
    mi_playwright = lanzar_playwright() 
    navegador = mi_playwright.chromium.launch(headless=oculto)
    return navegador

def abrir_pagina(navegador:Browser,url:str,resolucion=[1920,1080]):
    resolution = {"width": resolucion[0], "height": resolucion[1]}
    pagina = navegador.new_page()
    # pagina.set_viewport_size(resolution)
    pagina.goto(url)    
    return pagina

def obtener_web_masvoz(seccion='ESTADÍSTICAS',maximizado=False,navegador=None,pagina=None):
    
    """
    Puedes elegir la seccion en la cual abrir la web de MasVoz, ['INICIO','AGENDA','SKILLS','SUPERVISIÓN','ESTADÍSTICAS','DETALLES','CUENTA']
    """
    seccionesDisponibles = ['INICIO','AGENDA','SKILLS','SUPERVISIÓN','ESTADÍSTICAS','DETALLES','CUENTA']
    seccion = seccion.upper()
    if(seccion not in seccionesDisponibles):
        seccion = 'ESTADÍSTICAS'
        if('silencioso' in globals()):
            if(silencioso==False):
                print('Has elegido una sección que no existe en la web, se ha establecido por defecto "ESTADÍSTICAS"')
           
    
    user = 'vy_konecta_sup01'
    password = 'Konecta27!'
    # driver = abrir_navegador()
    password = 'Konecta27!'
    if navegador == None:
        navegador = abrir_navegador()
    
    if pagina == None:
        pagina = abrir_pagina(navegador=navegador,url=url)
        pagina.get_by_placeholder("Nombre de usuario").fill(user)
        pagina.get_by_placeholder("Contraseña de usuario").fill(password)
        pagina.get_by_text("Entrar",exact=True).click()
    #compruebo si la sección ya está activada
    match seccion:
            case 'INICIO': id_elemento = '#main-btn-inicio'
            case 'ESTADÍSTICAS': id_elemento = '#main-btn-estadisticas'
            case 'DETALLES': id_elemento = '#main-btn-detalle'
            case 'CUENTA': id_elemento = '#main-btn-cuenta'
            case 'SUPERVISIÓN': id_elemento = '#detalle-main-btn-supervision'
            case 'SKILLS': id_elemento = '#main-btn-skills'
    
    clase_encontrada = pagina.locator(id_elemento).get_attribute("class")
    if clase_encontrada != 'active':  # Verificar si está ya seleccionada la seccion
        pagina.locator(id_elemento).click()
          
    match seccion:
        case 'AGENDA': id_elemento = '#agenda_0'
        case 'ESTADÍSTICAS': id_elemento = '#estadisticas_0'
        case 'DETALLES': id_elemento = '#detalle-llamadas_0'
        case 'CUENTA': id_elemento = '#cuenta_0'
        case 'SUPERVISIÓN': id_elemento = '#supervision_0'
        case 'SKILLS': id_elemento = '#skills_0'
        case 'INICIO': id_elemento = '#inicio_0'
        case _:id_elemento = seccion
        
    try:
        frame = pagina.frame_locator(id_elemento)    
        if('silencioso' in globals()):
            if(silencioso==False):
                print('Entro al Iframe para poder seguir buscando elemento que están dentro del Iframe')
    except Exception as e:
        if('silencioso' in globals()):
            if(silencioso==False):
                print(f'No encontro el iframe')
        if pagina: pagina.close()
        return False
       
    return navegador,pagina,frame

  
       
def descargar_colas_masvoz(frame,pagina,hora_inicio:time, fecha_inicio=datetime.today().date()):
    finalizado_ok = False
   
   
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    hora_inicio = hora_inicio
    str_hora_inicio = hora_inicio.strftime("%H:%M:%S")
    tiempo_a_sumar = timedelta(minutes=29, seconds=59)
    fecha_hora_inicio = datetime.combine(fecha_inicio_filtro,hora_inicio)

    horaFin = fecha_hora_inicio + tiempo_a_sumar
    horaFin = horaFin.time()
    str_hora_fin = horaFin.strftime("%H:%M:%S")    
    frame.locator(".a_tab_colas").click()

    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))

    frame.locator('#int_timepicker_b_4').fill(str_hora_fin)
    frame.get_by_role('combobox').select_option('Cola')
    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_colas.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok

def descargar_tramos_masmoz(frame,pagina,fecha_inicio=datetime.today().date()):
    finalizado_ok = False
   
   
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  
    
    frame.locator(".a_tab_colas").click()
 
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
    
    frame.locator('#int_timepicker_a_4').fill(str(str_hora_inicio))
    frame.locator('#int_timepicker_b_4').fill(str(str_hora_fin))
    frame.get_by_role('combobox').select_option('Franjas de 30 minutos')
    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
 
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_tramos.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok

def descargar_colas_tramos_masvoz(frame,pagina,hora_inicio:time, fecha_inicio=datetime.today().date()):
    finalizado_ok = False    
   
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    hora_inicio = hora_inicio
    str_hora_inicio = hora_inicio.strftime("%H:%M:%S")
    tiempo_a_sumar = timedelta(minutes=29, seconds=59)
    fecha_hora_inicio = datetime.combine(fecha_inicio_filtro,hora_inicio)

    horaFin = fecha_hora_inicio + tiempo_a_sumar
    horaFin = horaFin.time()
    str_hora_fin = horaFin.strftime("%H:%M:%S")    

    frame.locator(".a_tab_colas").click()

    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))

    frame.locator('#int_timepicker_a_4').fill(str(str_hora_inicio))
    frame.locator('#int_timepicker_b_4').fill(str(str_hora_fin))
    frame.get_by_role('combobox').select_option('Cola')
    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_colas_tramos.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok

def descargar_colas_individual_tramos_masvoz(frame,pagina,fecha_inicio=datetime.today().date(),cola='Todas las Colas'):
    finalizado_ok = False    
   
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  

    frame.locator(".a_tab_colas").click()
    frame.locator('.btn.btn-block.btn-restablecer').click()
    
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))

    frame.locator('#int_timepicker_a_4').fill(str(str_hora_inicio))
    frame.locator('#int_timepicker_b_4').fill(str(str_hora_fin))
    frame.get_by_role('combobox').select_option('Franjas de 30 minutos')

    #compruebo si filtro avanzado está desplegado
    try: 
        frame.locator(".btn-group.bootstrap-select.show-tick.adv-colas.large-select").first.click(timeout=segundos_de_espera*1000)
    except:
        #muestro filtro avanzado
        frame.locator('.btn.btn-block.advanced-filter-btn').click()
        frame.locator(".btn-group.bootstrap-select.show-tick.adv-colas.large-select").first.click()
    else:
        #aplico filtro de cola
        frame.get_by_role("option",name=cola).first.click()


    #busca resultados
    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
        
   
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_colas_individual_tramos.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado la cola {cola} {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok

def descargar_actividad_por_agente(frame,pagina,fecha_inicio=datetime.today().date()):
    finalizado_ok = False
      
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime(" %d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  

    frame.locator(".tab_actividad-agentes").click()
    frame.locator('.btn.btn-block.btn-restablecer').click()
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        
    frame.locator('#timepicker3').clear()
    frame.locator('#timepicker3').fill(str_hora_inicio)
    frame.locator('#timepicker4').clear()
    frame.locator('#timepicker4').fill(str_hora_fin)
    frame.get_by_role('combobox').select_option('Agente')
    
    try:
        frame.get_by_role("button", name="Aceptar").click(timeout=1000) #pulso aceptar si muestra mensaje de que no hay datos
    except:
        pass

    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_actividad_por_agente.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok

def descargar_actividad_por_agente_cola (frame,pagina,fecha_inicio=datetime.today().date()):
    finalizado_ok = False
      
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")
   
    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  
    
    frame.locator(".tab_actividad-agentes").click()
    frame.locator('.btn.btn-block.btn-restablecer').click()
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        
    frame.locator('#timepicker3').clear()
    frame.locator('#timepicker3').fill(str_hora_inicio)
    frame.locator('#timepicker4').clear()
    frame.locator('#timepicker4').fill(str_hora_fin)
    frame.get_by_role('combobox').select_option('Cola y Agente')
    
    try:
        frame.get_by_role("button", name="Aceptar").click(timeout=1000) #pulso aceptar si muestra mensaje de que no hay datos
    except:
        pass

    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_actividad_por_agente_cola.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok

def descargar_estados_por_agente(frame,pagina,fecha_inicio=datetime.today().date()):
    #Solo deja descargar una vez, para saltar esta limitación hay que volver abrir el navegador    
    finalizado_ok = False
      
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  
    
    frame.locator(".tab_estado-agentes").click()
    frame.locator('.btn.btn-block.btn-restablecer').click()
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        
    frame.locator('#timepicker3').clear()
    frame.locator('#timepicker3').fill(str_hora_inicio)
    frame.locator('#timepicker4').clear()
    frame.locator('#timepicker4').fill(str_hora_fin)
    
    try:
        frame.get_by_role("button", name="Aceptar").click(timeout=1000) #pulso aceptar si muestra mensaje de que no hay datos
    except:
        pass

    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_estados_por_agente.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok

def descargar_agentes(frame,pagina,fecha_inicio=datetime.today().date()):
    finalizado_ok = False   
   
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")
   
    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  

    frame.locator(".a_tab_agentes").click()
    frame.locator('.btn.btn-block.btn-restablecer').click()
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        
    frame.locator('#timepicker3').clear()
    frame.locator('#timepicker3').fill(str_hora_inicio)
    frame.locator('#timepicker4').clear()
    frame.locator('#timepicker4').fill(str_hora_fin)
    frame.get_by_role('combobox').select_option('Agente')
    
    try:
        frame.get_by_role("button", name="Aceptar").click(timeout=1000) #pulso aceptar si muestra mensaje de que no hay datos
    except:
        pass

    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_agentes.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok
 
def descargar_listado_llamadas(frame,pagina,fecha_inicio=datetime.today().date()):
    finalizado_ok = False

    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")

    dia, mes, año = str_fecha_inicio_filtro.split('-')
    dia = dia.lstrip('0')
    mes = mes.lstrip('0')
    str_fecha_inicio_filtro = f"{dia}-{mes}-{año}"

    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    dia, mes, año = str_fecha_fin_filtro.split('-')
    dia = dia.lstrip('0')
    mes = mes.lstrip('0')
    str_fecha_fin_filtro = f"{dia}-{mes}-{año}"

    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  

    frame.locator(".a_tab_detalle").click()
    frame.locator('.btn.btn-block.btn-restablecer').click()
    
    #compruebo si filtro avanzado está desplegado
    try: 
        frame.locator('.advance-group-hangupby').get_by_title("Todos").check(timeout=1000)
    except:
        encontrado = False
    else:
        encontrado = True
    
    if not encontrado:
        frame.locator('.btn.btn-block.advanced-filter-btn').click()
    
    frame.locator('.form-incluir.form-inline').get_by_title("Emitidas").check()
    frame.locator('.advance-group-hangupby').get_by_title("Todos").check()
    frame.locator('.advance-group-include').get_by_title("Todos").check()
    
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
    frame.locator('#timepicker3').clear()
    frame.locator('#timepicker3').fill(str_hora_inicio)
    frame.locator('#timepicker4').clear()
    frame.locator('#timepicker4').fill(str_hora_fin)
    
    try:
        frame.get_by_role("button", name="Aceptar").click(timeout=1000) #pulso aceptar si muestra mensaje de que no hay datos
    except:
        pass

    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_listado_llamadas.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok
        

def descargar_listado_acd(frame,pagina,fecha_inicio=datetime.today().date()):
    finalizado_ok = False
    
    fecha_inicio_filtro = fecha_inicio
    str_fecha_inicio_filtro = fecha_inicio_filtro.strftime("%d-%m-%Y")

    dia, mes, año = str_fecha_inicio_filtro.split('-')
    dia = dia.lstrip('0')
    mes = mes.lstrip('0')
    str_fecha_inicio_filtro = f"{dia}-{mes}-{año}"

    fecha_fin_filtro = fecha_inicio
    str_fecha_fin_filtro = fecha_fin_filtro.strftime("%d-%m-%Y")

    dia, mes, año = str_fecha_fin_filtro.split('-')
    dia = dia.lstrip('0')
    mes = mes.lstrip('0')
    str_fecha_fin_filtro = f"{dia}-{mes}-{año}"

    str_hora_inicio = '00:00:00'
    str_hora_fin = '23:59:59'  

    frame.locator(".a_tab_acd").click()
    frame.locator('.btn.btn-block.btn-restablecer').click()
    
    if(fecha_inicio!=datetime.today().date()):
        frame.locator('#etime').clear()
        frame.locator('#etime').press_sequentially(str(str_fecha_fin_filtro))
        
        frame.locator('#stime').clear()
        frame.locator('#stime').press_sequentially(str(str_fecha_inicio_filtro))
    frame.locator('#timepicker3').clear()
    frame.locator('#timepicker3').fill(str_hora_inicio)
    frame.locator('#timepicker4').clear()
    frame.locator('#timepicker4').fill(str_hora_fin)
    
    try:
        frame.get_by_role("button", name="Aceptar").click(timeout=1000) #pulso aceptar si muestra mensaje de que no hay datos
    except:
        pass

    frame.get_by_text("Buscar").click(timeout=segundos_de_espera_descarga*1000)
    
    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga_aumentado*1000) as download_info:
        frame.get_by_role("link", name="CSV").click(timeout=segundos_de_espera_descarga_aumentado*1000)

    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_listado_acd.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo} Desde el {str_fecha_inicio_filtro} {str_hora_inicio} al {str_fecha_fin_filtro} {str_hora_fin} ")

    return finalizado_ok


def descargar_skills_agentes(frame,pagina):
    sleep(10)
    frame.locator(".a_tab_skills_agents").click()

    # Empieza a descargar y espera el archivo de descarga
    with pagina.expect_download(timeout=segundos_de_espera_descarga*1000) as download_info:
        frame.locator(".span12.filter-menu").get_by_role("button",name="Informe CSV").click(timeout=segundos_de_espera_descarga*1000)
    
    download = download_info.value
    nombre_archivo = os.path.join(ruta_destino,'Informe_listado_acd.csv')
    download.save_as(nombre_archivo)
    finalizado_ok = nombre_archivo
    print(f"Se ha exportado {nombre_archivo}")

    return finalizado_ok

def copiar_archivo(nombre_archivo,nombre_archivo_final:str,ruta_destino:str,intentos=0):
    while (intentos<10):
        try:
            shutil.copy(nombre_archivo, f"{ruta_destino}\\{nombre_archivo_final}")
        except PermissionError:
                intentos +=1
                print(f'El archivo {nombre_archivo} o {ruta_destino}\\{nombre_archivo_final} está bloqueado...intentos {intentos} de 10')
                sleep(segundos_de_espera)
                copiar_archivo(nombre_archivo=nombre_archivo,nombre_archivo_final=nombre_archivo_final,ruta_destino=ruta_destino,intentos=intentos)
        else:
            print(f'Archivo {os.path.basename(nombre_archivo)} movido a {ruta_destino}\\{nombre_archivo_final}')
            return True
        break
   
def mover_archivo(nombre_archivo,nombre_archivo_final:str,ruta_destino:str,intentos=0):
    while (intentos<10):
        try:
            shutil.move(nombre_archivo, f"{ruta_destino}\\{nombre_archivo_final}")
        except PermissionError:
                intentos +=1
                print(f'El archivo {nombre_archivo} o {ruta_destino}\\{nombre_archivo_final} está bloqueado...intentos {intentos} de 10')
                sleep(segundos_de_espera)
                mover_archivo(nombre_archivo=nombre_archivo,nombre_archivo_final=nombre_archivo_final,ruta_destino=ruta_destino,intentos=intentos)
        else:
            print(f'Archivo {os.path.basename(nombre_archivo)} movido a {ruta_destino}\\{nombre_archivo_final}')
            return True
        break

def guardar_csv(archivo,nombre_archivo_final:str,intentos=0):
    while (intentos<10):
        try:
            
            archivo.to_csv(nombre_archivo_final, index=False,sep=';')
        except PermissionError:
                intentos +=1
                print(f'El archivo {nombre_archivo_final} está bloqueado...intentos {intentos} de 10')
                sleep(segundos_de_espera)
                guardar_csv(archivo=archivo,nombre_archivo_final=nombre_archivo_final,intentos=intentos)
        else:
            print(f'Archivo {nombre_archivo_final} se ha guardado correctamente')
            return True
        break

def obtener_largo_archivo(archivo_origen,separacion=';',intentos=0):
    while (intentos<10):
        try:
        # Leer el archivo CSV en un DataFrame
            try:
                df = pd.read_csv(archivo_origen, index_col=0, sep=separacion)
            except UnicodeDecodeError:
                df = pd.read_csv(archivo_origen, index_col=0, sep=separacion,encoding='latin-1')        
           
        except PermissionError:
            intentos +=1
            print(f'El archivo {archivo_origen} está bloqueado...intentos {intentos} de 10')
            sleep(segundos_de_espera)
            obtener_largo_archivo(archivo_origen=archivo_origen,separacion=separacion,intentos=intentos)
       
        else:
            # Obtengo el largo del archivo
            largo = len(df)
            if('silencioso' in globals()):
                if(silencioso==False):
                    print(f'Al archivo {os.path.basename(archivo_origen)} tiene {largo} lineas de datos contando el encabezado')
            return largo
        break
   
def insertar_columna_csv(archivo_origen,nombre_columna,dato,separacion=';',separacion_destino=';',intentos=0):
    while (intentos<10):
        try:
        # Leer el archivo CSV en un DataFrame
            try:
                df = pd.read_csv(archivo_origen, index_col=0, sep=separacion)
            except UnicodeDecodeError:
                df = pd.read_csv(archivo_origen, index_col=0, sep=separacion,encoding='latin-1')        
            # Agregar las nuevas columnas
            df[nombre_columna] = dato
            df.to_csv(archivo_origen,sep=separacion_destino,encoding="utf-8")
        except PermissionError:
            intentos +=1
            print(f'El archivo {archivo_origen} está bloqueado...intentos {intentos} de 10')
            sleep(segundos_de_espera)
            insertar_columna_csv(archivo_origen=archivo_origen,nombre_columna=nombre_columna,separacion=separacion,separacion_destino=separacion_destino,intentos=intentos)
       
        else:
            print(f'Al archivo {os.path.basename(archivo_origen)} se le añadió la columna {nombre_columna} con el dato: {dato}')
            return True
        break

def acumular_datos(archivo_nuevo,archivo_acumulado,separador=";",intentos=0,formato_fecha='%Y-%m-%d'):
    while (intentos<10):
        try:
            df_nuevo = pd.read_csv(archivo_nuevo, index_col=0, sep=separador,parse_dates=["Fecha"],date_format=formato_fecha)
            df_nuevo["Fecha"] = df_nuevo["Fecha"].dt.strftime('%Y-%m-%d')
            if os.path.exists(archivo_acumulado):
                df_acumulado = pd.read_csv(archivo_acumulado, index_col=0, sep=separador)
                df_combinado = pd.concat([df_acumulado, df_nuevo], axis=0)
                df_combinado.to_csv(archivo_acumulado,sep=separador,encoding="utf-8")
            else:
                df_nuevo.to_csv(archivo_acumulado,sep=separador,encoding="utf-8")
           
           
        except PermissionError:
            intentos +=1
            print(f'El archivo {archivo_acumulado} está bloqueado...intentos {intentos} de 10')
            sleep(segundos_de_espera)
            acumular_datos(archivo_nuevo=archivo_nuevo,archivo_acumulado=archivo_acumulado,separador=separador,intentos=intentos,formato_fecha=formato_fecha)
        else:
            print(f'Los datos del archivo {os.path.basename(archivo_nuevo)} se han acumulado en el archivo {os.path.basename(archivo_acumulado)}')
        break

def eliminar_duplicados(archivo,columnas_unicas:list,separador=';',intentos=0):
    while (intentos<10):
        try:
            df = pd.read_csv(archivo,sep=separador)
            df = df.drop_duplicates(subset=columnas_unicas,keep='last') #Elimino duplicados, me quedo con el último
            df.to_csv(path_or_buf=archivo,sep=separador,encoding="utf-8",index=False)

        except PermissionError:
            intentos +=1
            print(f'El archivo {archivo} está bloqueado...intentos {intentos} de 10')
            sleep(3)
            eliminar_duplicados(archivo=archivo,columnas_unicas=columnas_unicas,separador=separador,intentos=intentos)
        except Exception as e:
            intentos +=1
            print(f"Error al procesar el archivo: {e}")
            sleep(3)
            eliminar_duplicados(archivo=archivo,columnas_unicas=columnas_unicas,separador=separador,intentos=intentos)
       
        else:

            print(f'Se han eliminado los registros duplicados al archivo {os.path.basename(archivo)} agrupado por:  {columnas_unicas}')
            return True
        break

def eliminar_registros_por_num_dias_atras(archivo,num_dias_atras:int,desde_la_fecha:datetime,separador=';',intentos=0):
    while (intentos<10):
        try:
            df = pd.read_csv(archivo,sep=separador)
            fecha_limite = (desde_la_fecha - timedelta(days=num_dias_atras)).strftime('%Y-%m-%d')
            #para averiguar cuantas se van a borrar
            cantidadRegistrosABorrar = len(df[df['Fecha'] < fecha_limite])

           
            df = df[df['Fecha'] >= fecha_limite]
            df.to_csv(path_or_buf=archivo,sep=separador,encoding="utf-8",index=False)
           
        except PermissionError:
            intentos +=1
            print(f'El archivo {archivo} está bloqueado...intentos {intentos} de 10')
            sleep(3)
            eliminar_registros_por_num_dias_atras(archivo=archivo,num_dias_atras=num_dias_atras,desde_la_fecha=desde_la_fecha,separador=separador,intentos=intentos)
        except Exception as e:
            intentos +=1
            print(f"Error al procesar el archivo: {e}")
            sleep(3)
            eliminar_registros_por_num_dias_atras(archivo=archivo,num_dias_atras=num_dias_atras,desde_la_fecha=desde_la_fecha,separador=separador,intentos=intentos)
       
        else:

            print(f'Se han eliminado {cantidadRegistrosABorrar} registros, los que tenían fecha inferior a {fecha_limite} que corresponden a los {num_dias_atras} dias antes de la fecha {desde_la_fecha}')
            return True
        break
 
def convertir_deltatime_a_time(tiempoEnSegundos:timedelta):
    horas = int(tiempoEnSegundos.total_seconds()/3600)
    tiempo_sobrante = tiempoEnSegundos.total_seconds()%3600
    minutos = int(tiempo_sobrante/60)
    segundos = int(tiempo_sobrante%60)
   
    return time(hour=horas,minute=minutos,second=segundos)

def obtener_conexion_bd(host,user,password,database,port=3306):
    host=host
    port=port
    user=user
    password=password
    database=database

    try:
        conexion_db = mysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
    except mysql.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
    else:
        return conexion_db

def lanzar_query(conexion_db,query):
    cursor = conexion_db.cursor()
    resultados = []
    cursor.execute(query)
   
    for x in cursor.fetchall():
        resultados.append(x)
    return resultados

def obtener_tramos_faltantes_bd(tabla,fecha_inicio,fecha_fin,columna_fecha,columna_tiempo,ultimo_tramo_del_dia=False,minutosPorTramo=30):
    lista_fechas_tramos_requeridas=[]
    lista_fechas_tramos_en_bd=[]
    lista_fechas_tramos_faltantes=[]
   
    ###Creo lista de fechas y horas entre fecha_inicio y fecha_fin
    #elimino segundo y milisegundos a fechas
    fecha_inicio = datetime.replace(fecha_inicio,second=0)
    fecha_inicio = datetime.replace(fecha_inicio,microsecond=0)
   
    fecha_fin = datetime.replace(fecha_fin,second=0)
    fecha_fin = datetime.replace(fecha_fin,microsecond=0)
    #en bbdd no debería estar subido el tramo que corresponde sacar ahora, por lo cual le quito 60 minutos y no 30
    ahora = datetime.now()-timedelta(minutes=60)
   
    #Se busca el tramo ya cerrado, si son 15:25, exporta tramo de 14:30 a 14:59:59

    horaActual = ahora.hour
    minutoActual = ahora.minute
   
   
    if ultimo_tramo_del_dia:
        tiempo_delta = timedelta(hours=24)
        fecha_inicio = datetime.combine(datetime.date(fecha_inicio),time(hour=23,minute=30))
        fecha_fin = datetime.combine(datetime.date(fecha_fin),time(hour=horaActual,minute=minutoActual))
    else:
        tiempo_delta = timedelta(minutes=minutosPorTramo)
       
        if(fecha_inicio.minute<minutosPorTramo):
            fecha_inicio = datetime.replace(fecha_inicio,minute=0)
        else:
            fecha_inicio = datetime.replace(fecha_inicio,minute=minutosPorTramo)
           
       
    fecha_inicioWhile = fecha_inicio
    while fecha_inicioWhile <= fecha_fin:
        if(fecha_inicioWhile.hour>=primer_tramo_del_dia):#se añade la fecha y hora si es superior a la variable primer_tramo_del_dia, normalmente seteado en 8 para que exporte tramos de 8 hasta 23,30 hrs.
            lista_fechas_tramos_requeridas.append(fecha_inicioWhile)
        fecha_inicioWhile += tiempo_delta
   
   
    ###Creo lista de fechas y horas de la bd navicat
    query = f"SELECT DISTINCT `{columna_fecha}`, `{columna_tiempo}` FROM `{tabla}` WHERE `{columna_fecha}` BETWEEN '{fecha_inicio.strftime('%Y-%m-%d')}' AND '{fecha_fin.strftime('%Y-%m-%d')}'"
    conexion_db = obtener_conexion_bd(host,user,password,database)
    resultado_query = lanzar_query(conexion_db=conexion_db,query=query)

    #uno columnas fecha y hora, en una sola para obtener datetime
    for row in resultado_query:
        lista_fechas_tramos_en_bd.append(datetime.combine(row[0],convertir_deltatime_a_time(row[1])))
       
       
       
    for fecha in lista_fechas_tramos_requeridas:
        if(fecha not in lista_fechas_tramos_en_bd):
            lista_fechas_tramos_faltantes.append(fecha)

    if (len(lista_fechas_tramos_faltantes)>0):
        print(f'En la tabla {tabla} faltan las siguientes fechas:')
        for fecha in reversed(lista_fechas_tramos_faltantes):
            print(f'{fecha}')
    else:
        print(f'En la tabla {tabla} no faltan fechas ni tramos entre {fecha_inicio} and {fecha_fin}')
    return lista_fechas_tramos_faltantes

def obtener_tramos_faltantes_csv_acumulados(archivo,fecha_inicio,fecha_fin,columna_fecha,columna_tiempo,separador=';',ultimo_tramo_del_dia=False,minutosPorTramo=30,formato_fecha='%Y-%m-%d'):
   
    lista_fechas_tramos_requeridas=[]
    lista_fechas_tramos_acumuladas=[]
    lista_fechas_tramos_faltantes=[]
   
    ###Creo lista de fechas y horas entre fecha_inicio y fecha_fin
    #elimino segundo y milisegundos a fechas
    fecha_inicio = datetime.replace(fecha_inicio,second=0)
    fecha_inicio = datetime.replace(fecha_inicio,microsecond=0)
   
    fecha_fin = datetime.replace(fecha_fin,second=0)
    fecha_fin = datetime.replace(fecha_fin,microsecond=0)
    ahora = datetime.now()-timedelta(minutes=30)
   
    #Se busca el tramo ya cerrado, si son 15:25, exporta tramo de 14:30 a 14:59:59

    horaActual = ahora.hour
    minutoActual = ahora.minute
   
   
    if ultimo_tramo_del_dia:
        tiempo_delta = timedelta(hours=24)
        fecha_inicio = datetime.combine(datetime.date(fecha_inicio),time(hour=23,minute=30))
        fecha_fin = datetime.combine(datetime.date(fecha_fin),time(hour=horaActual,minute=minutoActual))
    else:
        tiempo_delta = timedelta(minutes=minutosPorTramo)
       
        if(fecha_inicio.minute<minutosPorTramo):
            fecha_inicio = datetime.replace(fecha_inicio,minute=0)
        else:
            fecha_inicio = datetime.replace(fecha_inicio,minute=minutosPorTramo)
           
       
    fecha_inicioWhile = fecha_inicio
    while fecha_inicioWhile <= fecha_fin:
        if(fecha_inicioWhile.hour>=primer_tramo_del_dia):#se añade la fecha y hora si es superior a la variable primer_tramo_del_dia, normalmente seteado en 8 para que exporte tramos de 8 hasta 23,30 hrs.
            lista_fechas_tramos_requeridas.append(fecha_inicioWhile)
        fecha_inicioWhile += tiempo_delta
       
    ###Creo lista de fechas y horas del acumulado
    if os.path.exists(archivo):
        df = pd.read_csv(archivo,sep=separador)
        if(columna_tiempo!=None):
            df = df[[columna_fecha,columna_tiempo]]
            columnas_unicas = [columna_fecha,columna_tiempo]
        else:
            df = df[[columna_fecha]]
            columnas_unicas = [columna_fecha]       
   
        df = df.drop_duplicates(subset=columnas_unicas,keep='last')
   
        #Recorro las fechas obtenidas del archivo acumulado y las transforma a datetime
        for fila in df.itertuples():
            fecha = datetime.strptime(fila[1].split(" ")[0],formato_fecha)#Obtengo la fecha de la columna Fecha
            if(columna_tiempo!=None):
                hora = datetime.strptime(fila[2],'%H:%M:%S').time() #Obtendo la hora de la columna Hora si la tuviese
            else:
                hora = time(hour=23,minute=30)
            lista_fechas_tramos_acumuladas.append(datetime.combine(fecha,hora))

       
       
    for fecha in lista_fechas_tramos_requeridas:
        if(fecha not in lista_fechas_tramos_acumuladas):
            lista_fechas_tramos_faltantes.append(fecha)

    if (len(lista_fechas_tramos_faltantes)>0):
        print(f'En el archivo {archivo} faltan las siguientes fechas:')
        for fecha in reversed(lista_fechas_tramos_faltantes):
            print(f'{fecha}')
    else:
        print(f'En el archivo {archivo} no faltan fechas ni tramos entre {fecha_inicio} and {fecha_fin}')
    return lista_fechas_tramos_faltantes
           
def obtener_tramos_con_cola_faltantes_csv_acumulados(archivo,fecha_inicio,fecha_fin,columna_fecha,separador=';',formato_fecha='%Y-%m-%d'):
   
    lista_fechas_tramos_requeridas=[]
    lista_fechas_tramos_acumuladas=[]
    lista_fechas_tramos_faltantes=[]
   
    ###Creo lista de fechas y horas entre fecha_inicio y fecha_fin
    #elimino segundo y milisegundos a fechas
    fecha_inicio = datetime.replace(fecha_inicio,second=0)
    fecha_inicio = datetime.replace(fecha_inicio,microsecond=0)
   
    fecha_fin = datetime.replace(fecha_fin,second=0)
    fecha_fin = datetime.replace(fecha_fin,microsecond=0)
    ahora = datetime.now()-timedelta(minutes=30)
   
    #Se busca el tramo ya cerrado, si son 15:25, exporta tramo de 14:30 a 14:59:59

    horaActual = ahora.hour
    minutoActual = ahora.minute
   
    tiempo_delta = timedelta(hours=24)
    fecha_inicio = datetime.combine(datetime.date(fecha_inicio),time(hour=23,minute=30))
    fecha_fin = datetime.combine(datetime.date(fecha_fin),time(hour=horaActual,minute=minutoActual))
           
       
    for cola in lista_colas:
        fecha_inicioWhile = fecha_inicio
        while fecha_inicioWhile <= fecha_fin:
            if(fecha_inicioWhile.hour>=primer_tramo_del_dia):#se añade la fecha y hora si es superior a la variable primer_tramo_del_dia, normalmente seteado en 8 para que exporte tramos de 8 hasta 23,30 hrs.
                lista_fechas_tramos_requeridas.append([fecha_inicioWhile,cola])
            fecha_inicioWhile += tiempo_delta
       
    ###Creo lista de fechas y horas del acumulado
    if os.path.exists(archivo):
        df = pd.read_csv(archivo,sep=separador)

        df = df[[columna_fecha,'Cola']]
        columnas_unicas = [columna_fecha,'Cola']       
   
        df = df.drop_duplicates(subset=columnas_unicas,keep='last')
   
        #Recorro las fechas obtenidas del archivo acumulado y las transforma a datetime
        for fila in df.itertuples():
            fecha = datetime.strptime(fila[1].split(" ")[0],formato_fecha)#Obtengo la fecha de la columna Fecha
            hora = time(hour=23,minute=30)
            lista_fechas_tramos_acumuladas.append([datetime.combine(fecha,hora),fila[2]])

       
       
    
    for fecha,cola in lista_fechas_tramos_requeridas:
        encontrado = False
        for fecha1,cola1 in lista_fechas_tramos_acumuladas:
            if cola == cola1:
                if fecha == fecha1:
                    encontrado = True
                    break
                    
            

        if not encontrado:
            lista_fechas_tramos_faltantes.append([fecha,cola])
    lista_fechas_tramos_faltantes.sort()
    if (len(lista_fechas_tramos_faltantes)>0):
        print(f'En el archivo {archivo} faltan las siguientes fechas:')
        for fecha in reversed(lista_fechas_tramos_faltantes):
            print(f'{fecha[0].strftime(format='%d-%m-%Y')} cola {fecha[1]} ')
        print(f"Faltan {len(lista_fechas_tramos_faltantes)} exportados por cola_individual_tramo")

    else:
        print(f'En el archivo {archivo} no faltan fechas ni tramos entre {fecha_inicio} and {fecha_fin}')
    
    return lista_fechas_tramos_faltantes
           

def exportar_tramos_faltantes(lista_informes=lista_informes_a_sacar):
    tipoInforme = 'colas'
    if(tipoInforme in lista_informes):
        print('Buscando tramos que faltan en el acumulado colas')
        lista_fechas_tramos_faltantes= []
        archivo = os.path.join(ruta_destino,'Informe_colas_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_colas)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha_hora_inicio=fecha,lista_informes=['colas'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)        
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_tramos_faltantes
    

    tipoInforme = 'tramos'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el acumulado tramos')
        lista_fechas_tramos_faltantes= []
        archivo = os.path.join(ruta_destino,'Informe_tramos_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_tramos)
        
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha_hora_inicio=fecha,lista_informes=['tramos'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
            else:
                fecha_inicio += timedelta(minutes=30)        
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_tramos_faltantes
    
    
    tipoInforme = 'colas_tramos'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el acumulado colas_tramos')
        lista_fechas_tramos_faltantes= []    
        archivo = os.path.join(ruta_destino,'Informe_colas_tramos_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_colas_tramos)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo',ultimo_tramo_del_dia=False)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha_hora_inicio=fecha,lista_informes=['colas_tramos'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_tramos_faltantes
    
    
    tipoInforme = 'actividad_por_agente'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el acumulado actividad_por_agente')
        lista_fechas_tramos_faltantes= []    
        archivo = os.path.join(ruta_destino,'Informe_actividad_por_agente_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_actividad_por_agente)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha,lista_informes=['actividad_por_agente'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_actividad_por agente_faltantes
    
    
    tipoInforme = 'actividad_por_agente_cola'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el acumulado actividad_por_agente_cola')
        lista_fechas_tramos_faltantes= []    
        archivo = os.path.join(ruta_destino,'Informe_actividad_por_agente_cola_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_actividad_por_agente_cola)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha,lista_informes=['actividad_por_agente_cola'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_actividad_por agente_faltantes
        
    
    tipoInforme = 'estados_por_agente'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el acumulado estados_por_agente')
        lista_fechas_tramos_faltantes= []    
        archivo = os.path.join(ruta_destino,'Informe_estados_por_agente_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_estados_por_agente)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha,lista_informes=['estados_por_agente'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_actividad_por agente_faltantes
    
    
    tipoInforme = 'agentes'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el acumulado agente')
        lista_fechas_tramos_faltantes= []    
        archivo = os.path.join(ruta_destino,'Informe_agentes_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_agentes)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha,lista_informes=['agentes'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_actividad_por agente_faltantes
    
    
    tipoInforme = 'listado_llamadas'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el listado_llamadas')
        lista_fechas_tramos_faltantes= []    
        archivo = os.path.join(ruta_destino,'Informe_listado_llamadas_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_listado_llamadas)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha,lista_informes=['listado_llamadas'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_actividad_por agente_faltantes
    
    
    tipoInforme = 'listado_acd'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el listado_acd')
        lista_fechas_tramos_faltantes= []    
        archivo = os.path.join(ruta_destino,'Informe_listado_acd_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_listado_acd)
        lista_fechas_tramos_faltantes = obtener_tramos_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha',columna_tiempo='Tramo_actualizado',ultimo_tramo_del_dia=True)
        for fecha in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha,lista_informes=['listado_acd'])
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
    
            else:
                fecha_inicio += timedelta(minutes=30)
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_actividad_por agente_faltantes

    tipoInforme = 'colas_individual_tramos'
    if(tipoInforme in lista_informes):   
        print('Buscando tramos que faltan en el colas_individual_tramos')
        lista_fechas_tramos_faltantes= []
        archivo = os.path.join(ruta_destino,'Informe_colas_individual_tramos_acumulado.csv')
        fecha_fin = datetime.now()-timedelta(minutes=30)
        fecha_inicio = fecha_fin - timedelta(days=num_dias_para_acumular_colas_individual_tramos)
        
        lista_fechas_tramos_faltantes = obtener_tramos_con_cola_faltantes_csv_acumulados(archivo=archivo,fecha_inicio=fecha_inicio,fecha_fin=fecha_fin,columna_fecha='Fecha')
        lista_fechas_tramos_faltantes.sort()
        for fecha,cola in reversed(lista_fechas_tramos_faltantes):
            if datetime.now().minute + 1 in lista_minutos:
                print("Se detiene la recuperacion de tramos faltantes porque empieza un nuevo tramo")
                break
            try:  
                if(fecha.hour>=primer_tramo_del_dia):
                    robot_informes_masvoz(fecha_hora_inicio=fecha,lista_informes=['colas_individual_tramos'],cola = cola)
            except Exception as error:
                logger.error("Errror:", exc_info=error)
                print(f'Volvemos a intentar con la fecha {fecha_inicio}')
            else:
                fecha_inicio += timedelta(minutes=30)        
            # return False #con este break hago que solo se ejecute la primera fecha encontrada, ya que llama a la funcion robot_informes_masvoz, y esa función al finalizar vuelve a llamar a esta exportar_tramos_faltantes
    
        
def ejecutar_en_minutos(funcion_a_lanzar,lista_minutos=[1,31]):
    # matar_proceso('CHROME')
    primeraVuelta = True
    while True:
        horaInicial = datetime.now().time()
        horaInicial = time.replace(horaInicial,microsecond=0)
        segundos = horaInicial.second
        if(horaInicial.minute in lista_minutos) or primeraVuelta:
            primeraVuelta = False
            fecha_hora_inicio = fecha_hora_inicio=datetime.now()-timedelta(minutes=30)
            lista_informes_a_sacar_propia = deepcopy(lista_informes_a_sacar) #hago una copia de la lista original
            funcion_a_lanzar()
            horaFinal = datetime.now().time()
            horaFinal = time.replace(horaFinal,microsecond=0)
            segundos = horaFinal.second
            # matar_proceso('CHROME')
            print(f'Se ejecuto entre las {horaInicial} y {horaFinal}')
            # enviar_correo(asunto="Robot MasVoz",mensaje=f"Exportado realizado, se ejecutó entre las {horaInicial} y {horaFinal} con el tramo de las {fecha_hora_inicio}",destinatario=destinatarioCorreo)
        horaFinal = datetime.now().time()
        horaFinal = time.replace(horaFinal,microsecond=0)
        print(f'Son las {horaFinal}...Esperando minutos {lista_minutos}')
        matar_proceso('CHROME')
        sleep(60-segundos)

def enviar_correo(asunto, mensaje, destinatario):
    try:
        servidor = smtplib.SMTP("smtp.gmail.com", 587)
        servidor.starttls()
        servidor.login(miCorreo, miPass)
        # Creacion del mensaje
       
        message = EmailMessage()
        message["From"] = miCorreo
        message["To"] = destinatario
        message["Subject"] = asunto
        message.set_content(mensaje)

        # Envio del correo
        servidor.send_message(message)
        servidor.quit()
    except Exception as error:
        print(f"Error en la funcion enviar_correo:\nPara: {destinatario},\nAsunto: {asunto}\nMensaje: {mensaje}\nError:\n{error} ")
    else:
        print('Se ha enviado el correo correctamente')

def prueba_crear_tabla_columnas_variables():
    db = obtener_conexion_bd(host,user,password,database)
    rutaArchivo = 'C:\\Users\\berna\\Desktop\\MUESTRA\\Informe_colas_tramos_acumulado.csv'
    df = pd.read_csv(rutaArchivo,sep=';')
    columas = df.columns
    query = 'CREATE TABLE vv_Informe_colas_tramos_acumulado_subido_robot ('
    for columna in columas:
        query += f'`{columna}` VARCHAR(255),'
    query = query[:-1] + ');'
    resultado = lanzar_query(db,query)
    print (resultado)

def prueba_crear_tabla_fija():
    query = """CREATE TABLE prueba.`vv_informe_colas_tramos_a_subir_robot` (
  `Categoría` varchar(255) DEFAULT NULL,
  `Recibidas | Número` int DEFAULT NULL,
  `Recibidas | Duración` time DEFAULT NULL,
  `Recibidas | Duración media` time DEFAULT NULL,
  `Atendidas | Número` int DEFAULT NULL,
  `Atendidas | %` double DEFAULT NULL,
  `Atendidas | Duración` time DEFAULT NULL,
  `Atendidas | Duración media` time DEFAULT NULL,
  `Atendidas | Tramo 00-10` int DEFAULT NULL,
  `Atendidas | Tramo 10-20` int DEFAULT NULL,
  `Atendidas | Tramo 20-30` int DEFAULT NULL,
  `Atendidas | Tramo 30-60` int DEFAULT NULL,
  `Atendidas | Tramo >60` int DEFAULT NULL,
  `Atendidas | TME` time DEFAULT NULL,
  `Atendidas | TMC` time DEFAULT NULL,
  `Atendidas | TMH` time DEFAULT NULL,
  `Atendidas | TMO` time DEFAULT NULL,
  `Desbordadas | Por cantidad` int DEFAULT NULL,
  `Desbordadas | Por tiempo` int DEFAULT NULL,
  `Desbordadas | TOTAL` int DEFAULT NULL,
  `Desbordadas | %` double DEFAULT NULL,
  `Abandonadas | Número` int DEFAULT NULL,
  `Abandonadas | %` double DEFAULT NULL,
  `Abandonadas | Duración` time DEFAULT NULL,
  `Abandonadas | TMA` time DEFAULT NULL,
  `Abandonadas | Tramo 00-30` int DEFAULT NULL,
  `Abandonadas | Tramo 30-60` int DEFAULT NULL,
  `Abandonadas | Tramo >60` int DEFAULT NULL,
  `NdS | %` double DEFAULT NULL,
  `Fecha` date DEFAULT NULL,
  `Tramo` time DEFAULT NULL
) engine=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;'"""
    host="localhost"
    user='root'
    password='root'
    database='prueba'
    db = obtener_conexion_bd(host,user,password,database)
    resultado = lanzar_query(db,query)
    print (resultado)

def robot_informes_masvoz(fecha_hora_inicio=datetime.now()-timedelta(minutes=30),lista_informes=lista_informes_a_sacar, cola = None):
    # navegador,pagina,frame = obtener_web_masvoz()
    navegador = None
    pagina = None
    frame = None
    #Si está fuera de horario, no exporta nada, primer_tramo_Del_dia está configurado en 9
    if(fecha_hora_inicio.hour<primer_tramo_del_dia):
        print(f'El tramo {fecha_hora_inicio.time()} está fuera del horario del servicio, el primer tramo es el de las {primer_tramo_del_dia}')
        return True
   
    #Se ajusta el tiempo para quedarse con la hora o la hora y media, según en que minute estemos
   
    hora = fecha_hora_inicio.time()
    fechaDatos = fecha_hora_inicio.date()
   
    #Se busca el tramo ya cerrado, si son 15:25, exporta tramo de 14:30 a 14:59:59

    horaActual = hora.hour
    minutoActual = hora.minute
   
    if(minutoActual <30):
        hora_inicio = time(horaActual, 0, 0)
    else:
        hora_inicio = time(horaActual, 30, 0)
       
       
    fecha_hora_inicio_tramo = datetime.combine(fechaDatos,hora_inicio)
    if(fecha_hora_inicio_tramo>(datetime.now()-timedelta(minutes=30))):
        print(f'Se está solicitando actualizar con fecha {fecha_hora_inicio_tramo} y ese tramo aun no está cerrado ')
        return True
        # raise ValueError("La Fecha no puede ser superior a ahora mismo")
 

    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """
    tipoInforme = 'colas'    
    columnas_unicas = ['Categoría','Fecha']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')

    if(tipoInforme in lista_informes):
        print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
        navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)
        archivoDescargado = descargar_colas_masvoz(frame=frame,pagina=pagina,hora_inicio=hora_inicio,fecha_inicio=fechaDatos)
        if(archivoDescargado!=False):
            hora_inicio = time.replace(hora_inicio,second=0)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
            largo = 0
            largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            if largo>3:
                acumular_datos(archivo_nuevo=archivoDescargado,archivo_acumulado=archivo_acumulado)
                eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_colas,desde_la_fecha=fechaDatos)
                
        else:
            print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')
        
        lista_informes.remove(tipoInforme)
        

    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """
    tipoInforme = 'tramos'
    columnas_unicas = ['Categoría','Fecha']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')

    if(tipoInforme in lista_informes):
        print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
        navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)
        archivoDescargado = descargar_tramos_masmoz(frame=frame,pagina=pagina,fecha_inicio=fechaDatos)
        if(archivoDescargado!=False):
            hora_inicio = time.replace(hora_inicio,second=0)
            nombre_archivo_finalUnico = f"Informe_{tipoInforme}_{str(hora_inicio).replace(':','_')}.csv"
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
            largo = 0
            largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            if largo>3:
                acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado)
                eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_tramos,desde_la_fecha=fechaDatos)
                
        else:
            print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

        lista_informes.remove(tipoInforme)

    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """    
    tipoInforme = 'colas_tramos'  
    columnas_unicas = ['Categoría','Fecha','Tramo']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')

    if(tipoInforme in lista_informes):
        print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
        navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)
        archivoDescargado = descargar_colas_tramos_masvoz(frame=frame,pagina=pagina,hora_inicio=hora_inicio,fecha_inicio=fechaDatos)
        if(archivoDescargado!=False):
            hora_inicio = time.replace(hora_inicio,second=0)
            nombre_archivo_finalUnico = f"Informe_{tipoInforme}_{str(hora_inicio).replace(':','_')}.csv"
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo',dato=hora_inicio)
            largo = 0
            largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            if largo>3:
                acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado)
                eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_colas_tramos,desde_la_fecha=fechaDatos)

        else:
            print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

        lista_informes.remove(tipoInforme)

        """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """

    tipoInforme = 'colas_individual_tramos'
    columnas_unicas = ['Categoría','Fecha','Cola']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')


    if(tipoInforme in lista_informes):
        if fecha_hora_inicio.date() == datetime.now().date():
            print("No se exportan datos de Hoy en colas_individual_tramos")
        else:
            print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
            navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)

            archivoDescargado = descargar_colas_individual_tramos_masvoz(frame=frame,pagina=pagina,fecha_inicio=fechaDatos,cola=cola)
            if(archivoDescargado!=False):
                hora_inicio = time.replace(hora_inicio,second=0)
                nombre_archivo_finalUnico = f"Informe_{tipoInforme}_{str(hora_inicio).replace(':','_')}.csv"
                insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos)
                insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
                insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Cola',dato=cola)
                largo = 0
                largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
                if largo>3:
                    acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado)
                    eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                    eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_tramos,desde_la_fecha=fechaDatos)
                    
            else:
                print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

        lista_informes.remove(tipoInforme)

            
    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """
    tipoInforme = 'actividad_por_agente'
    columnas_unicas = ['Categoría','Fecha']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')

    if(tipoInforme in lista_informes):
        print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
        navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)
        archivoDescargado = descargar_actividad_por_agente(frame=frame,pagina=pagina,fecha_inicio=fechaDatos)
        if(archivoDescargado!=False):
            hora_inicio = time.replace(hora_inicio,second=0)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
            largo = 0
            largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            if largo>1:
                acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado)
                eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_actividad_por_agente,desde_la_fecha=fechaDatos)
        
        else:
            print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')
        
        lista_informes.remove(tipoInforme)
            
    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """
    tipoInforme = 'actividad_por_agente_cola'
    columnas_unicas = ['Categoría','Cola','Fecha']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')

    if(tipoInforme in lista_informes):
        print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
        navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)
        archivoDescargado = descargar_actividad_por_agente_cola(frame=frame,pagina=pagina,fecha_inicio=fechaDatos)
        if(archivoDescargado!=False):
            hora_inicio = time.replace(hora_inicio,second=0)
            nombre_archivo_finalUnico = f"Informe_{tipoInforme}_{str(hora_inicio).replace(':','_')}.csv"
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
            largo = 0
            largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            if largo>3:
                acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado)
                eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_actividad_por_agente_cola,desde_la_fecha=fechaDatos)
                
        else:
            print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

        lista_informes.remove(tipoInforme)

        
    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """
    tipoInforme = 'estados_por_agente'
    columnas_unicas = ['Agente','Fecha','Hora','Estado inicial','Estado final','Evento']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')

    if(tipoInforme in lista_informes):
        print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
        #este informe solo deja descargar una vez, luego hay que cerrar el navegador
        if pagina: pagina.close()
        pagina = None        
        # navegador = None
        navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)
        archivoDescargado = descargar_estados_por_agente(frame=frame,pagina=pagina,fecha_inicio=fechaDatos)
        if(archivoDescargado!=False):
            hora_inicio = time.replace(hora_inicio,second=0)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
            largo = 0
            largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            if largo>3:
                acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado,formato_fecha='%d-%m-%Y')
                eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_estados_por_agente,desde_la_fecha=fechaDatos)
                
        else:
            print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

        lista_informes.remove(tipoInforme)

   


    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """
    tipoInforme = 'agentes'
    columnas_unicas = ['Categoría','Fecha']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')

    if(tipoInforme in lista_informes):
        print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
        #este informe solo deja descargar una vez, luego hay que cerrar el navegador
        if pagina: pagina.close()
        pagina = None        
        # navegador = None
        navegador,pagina,frame = obtener_web_masvoz(navegador=navegador,pagina=pagina)
        archivoDescargado = descargar_agentes(frame=frame,pagina=pagina,fecha_inicio=fechaDatos)
        if(archivoDescargado!=False):
            hora_inicio = time.replace(hora_inicio,second=0)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos)
            insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
            largo = 0
            largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            if largo>3:
                acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado)
                eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_agentes,desde_la_fecha=fechaDatos)
                
        else:
            print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

        lista_informes.remove(tipoInforme)



    """
    IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
    """
    tipoInforme = 'listado_llamadas'
    columnas_unicas = ['ID Llamada','Servicio','Cuenta','Fecha']
    archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')      
       
    if any(item in lista_informes for item in ['listado_llamadas']):
        if tipoInforme in lista_informes:
            if(fechaDatos==datetime.today().date()) and datetime.now().hour <= 8:
                print(f"Para {tipoInforme} aun no hay datos el {fechaDatos.strftime('%d-%m-%Y')}")
            else:
                #este informe solo deja descargar una vez, luego hay que cerrar el navegador
                if pagina: pagina.close()
                pagina = None        
                # navegador = None
                navegador,pagina,frame = obtener_web_masvoz(seccion='DETALLES',navegador=navegador,pagina=pagina)
                print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
                archivoDescargado = descargar_listado_llamadas(frame,pagina,fecha_inicio=fechaDatos)
                if(archivoDescargado!=False and archivoDescargado != '0_Llamadas'):
                    hora_inicio = time.replace(hora_inicio,second=0)
                    insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
                    largo = 0
                    largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
                    if largo>3:
                        acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado,formato_fecha='%d-%m-%Y')
                        eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                        eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_agentes,desde_la_fecha=fechaDatos)
                        
                elif archivoDescargado == '0_Llamadas':
                    print(f'No hay llamadas para {fecha_hora_inicio} de informe {tipoInforme}')
                else:
                    print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

                lista_informes.remove(tipoInforme)

   
    if any(item in lista_informes for item in ['listado_acd']):
        

        """
        IMPORTANTE CONFIGURAR EL tipoInforme,columnas_unicas,archivo_acumulado
        """
        tipoInforme = 'listado_acd'
        columnas_unicas = ['ID Llamada','Servicio','Fecha']
        archivo_acumulado = os.path.join(ruta_destino,f'Informe_{tipoInforme}_acumulado.csv')
   
        if tipoInforme in lista_informes:
            if(fechaDatos==datetime.today().date()) and datetime.now().hour <= 8:
                print(f"Para {tipoInforme} aun no hay datos el {fechaDatos.strftime('%d-%m-%Y')}")
            else:
                #este informe solo deja descargar una vez, luego hay que cerrar el navegador
                if pagina: pagina.close()
                pagina = None        
                # navegador = None
                navegador,pagina,frame = obtener_web_masvoz(seccion='DETALLES',navegador=navegador,pagina=pagina)
                print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
                archivoDescargado = descargar_listado_acd(frame=frame,pagina=pagina,fecha_inicio=fechaDatos)
                if(archivoDescargado!=False):
                    hora_inicio = time.replace(hora_inicio,second=0)
                    insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Tramo_actualizado',dato=hora_inicio)
                    largo = 0
                    largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
                    if largo>3:
                        acumular_datos(archivo_nuevo=os.path.join(ruta_destino,f'Informe_{tipoInforme}.csv'),archivo_acumulado=archivo_acumulado,formato_fecha='%d-%m-%Y')
                        eliminar_duplicados(archivo=archivo_acumulado,columnas_unicas=columnas_unicas)
                        eliminar_registros_por_num_dias_atras(archivo=archivo_acumulado,num_dias_atras=num_dias_para_acumular_agentes,desde_la_fecha=fechaDatos)
                        
                else:
                    print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')
                    
                lista_informes.remove(tipoInforme)


    """
        IMPORTANTE Esto busca y exporta los usuarios de masvoz, solo se saca una vez por la mañana en el primer tramo o si no encuentra el archivo
    """

    existe_archivo = os.path.exists(os.path.join(ruta_destino,'informe_skills_agentes.csv'))
    fecha_hora_primer_tramo = datetime.combine(datetime.today(), time(primer_tramo_del_dia, 0, 0)) 
    if any(item in lista_informes for item in ['skills_agentes']) and (fecha_hora_primer_tramo == fecha_hora_inicio_tramo or not existe_archivo) :
        
        tipoInforme = 'skills_agentes'
   
        if(tipoInforme in lista_informes):
            print(f"\n\n{'#'*(len(tipoInforme)+37)}\n### Empezando a sacar el informe_{tipoInforme} ###\n{'#'*(len(tipoInforme)+37)}\n")
            navegador,pagina,frame = obtener_web_masvoz('SKILLS')
            archivoDescargado = descargar_skills_agentes(frame=frame,pagina=pagina)
            if(archivoDescargado):
                hora_inicio = time.replace(hora_inicio,second=0)
                insertar_columna_csv(archivo_origen=archivoDescargado,nombre_columna='Fecha',dato=fechaDatos,separacion=',')
                largo = 0
                largo = obtener_largo_archivo(archivo_origen=archivoDescargado)
            else:
                print(f'Hubo un error al sacar el tipo de informe {tipoInforme}')

            lista_informes.remove(tipoInforme)
    pagina.close()
    pagina = None
    frame = None
    # """
    # IMPORTANTE Esto busca y exporta los tramos que faltan en los acumulados
    # """  
    # try:
    #     exportar_tramos_faltantes(lista_informes=lista_informes_a_sacar)
    # except:
    #     exportar_tramos_faltantes(lista_informes=lista_informes_a_sacar)


def iniciarRobot():
    try:    
        ejecutar_en_minutos(funcion_a_lanzar=lanzar_varios_robot_informes_masvoz,lista_minutos=lista_minutos)
        
    except Exception as error:
        logger.error("Error:", exc_info=error)
        print(f"#ERROR\n#ERROR\nReiniciando Robot\n#ERROR\n#ERROR")
        iniciarRobot()


#SOLO SE LANZA SI ESTE ES EL MODULO PRINCIPAL


def lanzar_varios_robot_informes_masvoz():
    
    proceso_colas = threading.Thread(target=robot_informes_masvoz,args=(datetime.now()-timedelta(minutes=30),['colas','tramos','colas_tramos']))
    proceso_agentes = threading.Thread(target=robot_informes_masvoz,args=(datetime.now()-timedelta(minutes=30),['agentes']))
    proceso_actividad_agentes = threading.Thread(target=robot_informes_masvoz,args=(datetime.now()-timedelta(minutes=30),['actividad_por_agente','actividad_por_agente_cola']))
    proceso_estados_agente = threading.Thread(target=robot_informes_masvoz,args=(datetime.now()-timedelta(minutes=30),['estados_por_agente']))
    proceso_listado_llamadas = threading.Thread(target=robot_informes_masvoz,args=(datetime.now()-timedelta(minutes=30),['listado_llamadas']))
    proceso_listado_acd = threading.Thread(target=robot_informes_masvoz,args=(datetime.now()-timedelta(minutes=30),['listado_acd']))
    proceso_skills_agentes = threading.Thread(target=robot_informes_masvoz,args=(datetime.now()-timedelta(minutes=30),['skills_agentes']))
    
    lista_procesos = {
                        proceso_colas,
                        proceso_agentes,
                        proceso_actividad_agentes,
                        proceso_estados_agente,
                        proceso_listado_llamadas,
                        proceso_listado_acd,
                        proceso_skills_agentes
                        }

    
    for proceso in lista_procesos:
        proceso.start()
        
    for proceso in lista_procesos:
        proceso.join()
        
    proceso_acumular_colas = threading.Thread(target=exportar_tramos_faltantes,args=([['colas','tramos','colas_tramos']]))
    proceso_acumular_colas_individual = threading.Thread(target=exportar_tramos_faltantes,args=([['colas_individual_tramos']]))
    proceso_acumular_agentes = threading.Thread(target=exportar_tramos_faltantes,args=([['agentes']]))
    proceso_acumular_actividad_agentes = threading.Thread(target=exportar_tramos_faltantes,args=([['actividad_por_agente','actividad_por_agente_cola']]))
    proceso_acumular_estados_agente = threading.Thread(target=exportar_tramos_faltantes,args=([['estados_por_agente']]))
    proceso_acumular_listado_llamadas = threading.Thread(target=exportar_tramos_faltantes,args=([['listado_llamadas']]))
    proceso_acumular_listado_acd = threading.Thread(target=exportar_tramos_faltantes,args=([['listado_llamadas']]))
    
    
    lista_procesos = {proceso_acumular_colas,
                      proceso_acumular_colas_individual,
                      proceso_acumular_agentes,
                      proceso_acumular_actividad_agentes,
                      proceso_acumular_estados_agente,
                      proceso_acumular_listado_llamadas,
                      proceso_acumular_listado_acd
                      }
    
    for proceso in lista_procesos:
        proceso.start()
        
    for proceso in lista_procesos:
        proceso.join()
    
if __name__ == "__main__":

    iniciarRobot()

