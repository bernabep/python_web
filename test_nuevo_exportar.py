import os
from playwright.sync_api import sync_playwright,Browser


def abrir_navegador(oculto=False):
    playwright = sync_playwright().start()
    navegador = playwright.chromium.launch(headless=oculto)    
    return navegador

def abrir_pagina(navegador:Browser,url:str,resolucion=[1920,1080]):
    resolution = {"width": resolucion[0], "height": resolucion[1]}
    pagina = navegador.new_page()
    pagina.set_viewport_size(resolution)
    pagina.goto(url)
    
    return pagina

url = 'https://manager.masvoz.es/'
user = 'vy_konecta_sup01'
password = 'Konecta27!'

ruta_destino = f"\\\\bcnsmb01.grupokonecta.corp\\SERVICIOS\\BOLL_COMUN_ANALISTAS\\Importar\\Robot" #!ruta de destino donde se almacenan los archivos definitivos
ruta_destino = f"C:\\Users\\berna\\Desktop\\MUESTRA" #!ruta de destino donde se almacenan los archivos definitivos
tipoInforme = "colas"
nombre_archivo_final= f'Informe_{tipoInforme}.csv'

navegador = abrir_navegador()
pagina = abrir_pagina(navegador,url)
pagina.get_by_placeholder("Nombre de usuario").fill(user)
pagina.get_by_placeholder("Contraseña de usuario").fill(password)
pagina.get_by_text("Entrar").click()
pagina.get_by_text("Estadísticas").click()
frame = pagina.frame_locator('#estadisticas_0')
frame.locator(".a_tab_colas").click()
frame.locator('#stime').clear()
frame.locator('#stime').press_sequentially('10-07-2024')
frame.get_by_text("Periodo desde : : hasta : : Recibidas Emitidas Llamadas SMS").first.click()
frame.locator('#etime').clear()
frame.locator('#etime').press_sequentially('10-07-2024')
frame.get_by_role('combobox').select_option('Franjas de 30 minutos')
frame.get_by_text("Buscar").click(timeout=60000)

# Start waiting for the download
with pagina.expect_download() as download_info:
    frame.get_by_role("link", name="CSV").click()
    # Perform the action that initiates download

download = download_info.value
download.save_as(os.path.join(ruta_destino,nombre_archivo_final))



    
    
