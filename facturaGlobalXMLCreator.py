from applicationinsights import TelemetryClient
import xml.etree.ElementTree as ET
from connectionController import *
from Entities.ControlDBEntities import ControlFacturaGlobal
import time
import sys 

logging = TelemetryClient(APPINSIGHT_INSTRUMENTATION_HEY)

def crearXML(ano, mes, tienda):
    try:
        fecha_inicio = time.time()
        root = crearRoot()
        crearNodoInformacionGlobal(ano, mes,root)
        crearNodoEmisor(root)
        crearNodoReceptor(root)
        conteoTickets = crearNodoConceptos(ano, mes, tienda,root)
        crearNodoImpuestosRoot(root)
        blob = getBlobClient(ano, mes, tienda)
        data = ET.tostring(root, xml_declaration=True, encoding='UTF-8')
        blob.upload_blob(data)
        fecha_fin = time.time()
        registrarFacturaGlobalMongo(blob.url, fecha_inicio, fecha_fin, conteoTickets, ano, mes, tienda)
    except Exception as e:
        logging.track_exception(*sys.exc_info(), properties={ 'ano': ano, 'mes' : mes, 'tienda': tienda })
        logging.flush()
        raise Exception(e)

def registrarFacturaGlobalMongo(url, fecha_inicio, fecha_fin, ticketsEnFactura, ano, mes, tienda):
    mongoDBControl = getMongoControllerConnection()
    elapsed = fecha_fin - fecha_inicio
    controlFactura = { "elapsedSeconds" : elapsed, 
                       "fechaInicio" : datetime.fromtimestamp(fecha_inicio), 
                       "fechaFin" : datetime.fromtimestamp(fecha_fin), 
                       "url": url,
                       "ticketsEnFactura": ticketsEnFactura,
                       "Año":ano,
                       "Mes": mes,
                       "Tienda": tienda
                     }
    mongoDBControl.insert_one(controlFactura)

def crearNodoImpuestosRoot(padre):
    Impuestos = ET.SubElement(padre,'cfdi:Impuestos')
    Impuestos.set('TotalImpuestosTrasladados','__TotalImpuestosTrasladados__')
    crearNodoTrasladosRoot(Impuestos)
    
def crearNodoTrasladosRoot(padre):
    traslados = ET.SubElement(padre,'cfdi:Traslados')
    crearNodoTrasladoRoot(traslados, "__base__", "__Impuesto__", "__TipoFactor__", "__TasaOCuota__", "__Importe__" )
    crearNodoTrasladoRoot(traslados, "__base__", "__Impuesto__", "__TipoFactor__", "__TasaOCuota__", "__Importe__" )
    crearNodoTrasladoRoot(traslados, "__base__", "__Impuesto__", "__TipoFactor__", "__TasaOCuota__", "__Importe__" )

def crearNodoTrasladoRoot(padre, Base, Impuesto, TipoFactor, TasaOCuota, Importe):
    traslado = ET.SubElement(padre,'cfdi:Traslado')
    traslado.set('Base', Base)
    traslado.set('Impuesto', Impuesto)
    traslado.set('TipoFactor', TipoFactor)
    traslado.set('TasaOCuota',TasaOCuota)
    traslado.set('Importe',Importe)

def crearNodoTraslado(padre, ticket):
    tasaCuota = 0.160000
    importe  =  float(ticket['Content']['TotalTicket']) * tasaCuota
    traslado = ET.SubElement(padre,'cfdi:Traslado')
    traslado.set('Base', f"{ticket['Content']['TotalTicket']}")
    traslado.set('Impuesto', '__Impuesto__')
    traslado.set('TipoFactor', '__TipoFactor__')
    traslado.set('TasaOCuota',"{:.6f}".format(tasaCuota))
    traslado.set('Importe',"{:.2f}".format(importe))

def crearNodoTraslados(padre, ticket):
    traslados = ET.SubElement(padre,'cfdi:Traslados')
    crearNodoTraslado(traslados, ticket)

def crearNodoImpuestos(padre, ticket):
    impuesto = ET.SubElement(padre,'cfdi:Impuestos')
    crearNodoTraslados(impuesto, ticket)

def crearNodoConcepto(padre, ticket):
    concepto = ET.SubElement(padre,'cfdi:Concepto')
    concepto.set('ClaveProdServ','__ClaveProdServ__') 
    concepto.set('NoIdentificacion',ticket['_id'])
    concepto.set('Cantidad','__Cantidad__')
    concepto.set('ClaveUnidad','__ClaveUnidad__')
    concepto.set('Descripcion','__Descripcion__')
    concepto.set('ValorUnitario',f"{ticket['Content']['TotalTicket']}")
    concepto.set('Importe',f"{ticket['Content']['TotalTicket']}")
    concepto.set('ObjetoImp', '__ObjetoImp__')
    crearNodoImpuestos(concepto, ticket)

def crearNodoConceptos(ano, mes, tienda, padre):
    conceptos = ET.SubElement(padre,'cfdi:Conceptos') 
    db = "pruebasConexion"
    collectionName = f"ticketsRaw{tienda}"
    ticketsPorTienda = get_Connection(ano, mes,collectionName,db)
    conteo = 0
    for ticket in ticketsPorTienda.find():
        crearNodoConcepto(conceptos, ticket)
        conteo += 1

    return conteo

def crearNodoReceptor(padre):
    receptor = ET.SubElement(padre,'cfdi:Receptor')
    receptor.set('Rfc','__Rfc__')
    receptor.set('UsoCFDI','__UsoCFDI__')
    receptor.set('RegimenFiscalReceptor','__RegimenFiscalReceptor__')

def crearNodoEmisor(padre):
    emisor = ET.SubElement(padre,'cfdi:Emisor')
    emisor.set('Rfc','__Rfc__')
    emisor.set('Nombre','__Nombre__')
    emisor.set('RegimenFiscal','__RegimenFiscal__')

def crearNodoInformacionGlobal(ano, mes, padre):
    informacionGlobal = ET.SubElement(padre,'cfdi:InformacionGlobal')
    informacionGlobal.set('Periodicidad','__periodicidad__')
    informacionGlobal.set('Meses',mes)
    informacionGlobal.set('Año',ano)

def crearRoot():
    root = ET.Element('cfdi:Comprobante')
    root.set('xmlns:tfd','http://www.sat.gob.mx/TimbreFiscalDigital')
    root.set('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance')
    root.set('xmlns:ns','https://www.oxxo.com/ns/addenda/oxxo/1')
    root.set('xmlns:implocal','http://www.sat.gob.mx/implocal')
    root.set('Serie','__Serie__')
    root.set('NoCertificado','__NoCertificado__')
    root.set('Folio','__Folio__')
    root.set('FormaPago','__FormaPago__')
    root.set('SubTotal','__SubTotal__')
    root.set('Moneda','__Moneda__')
    root.set('Total','__Total__')
    root.set('TipoDeComprobante','__TipoDeComprobante__')
    root.set('Exportacion','__Exportacion__')
    root.set('MetodoPago','__MetodoPago__')
    root.set('LugarExpedicion','__LugarExpedicion__')
    root.set('xsi:schemaLocation','http://www.sat.gob.mx/cfd/4 http://www.sat.gob.mx/sitio_internet/cfd/4/cfdv40.xsd http://www.sat.gob.mx/implocal http://www.sat.gob.mx/sitio_internet/cfd/implocal/implocal.xsd')
    root.set('Version','4.0')
    root.set('Fecha','__Fecha__')
    root.set('xmlns:cfdi','http://www.sat.gob.mx/cfd/4')
    return root