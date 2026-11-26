import { leer, pedir } from "@/api/cliente"
import { L } from "@/api/rutas"

export interface EstadoDelivery {
  stream: string
  estado: string
  shards?: number
  retencion_horas?: number
  arn?: string
  activo: boolean
  mensaje?: string
}

export interface ResultadoImportacion {
  resultado: string
  stream: string
  cantidad_solicitada: number
  pedido_ids: string[]
  enviados: number
  fallidos: number
  duracion_ms: number
  plataformas: string[]
  mensaje: string
}

export const estadoDelivery = (localId: string): Promise<EstadoDelivery> =>
  leer<EstadoDelivery>(`${L(localId)}/delivery/importar`)

export const importarDelivery = (localId: string, cantidad: number): Promise<ResultadoImportacion> =>
  pedir<ResultadoImportacion>("POST", `${L(localId)}/delivery/importar`, {
    cuerpo: { cantidad },
  }).then((r) => r.datos)


export interface PedidoDeliveryPersistido {
  pedido_id: string
  plataforma: string
  total: number
  n_items: number
  recibido_en: string
  estado: string
}

export interface SeguimientoDelivery {
  consultados: number
  persistidos: number
  pendientes: number
  claves_sin_procesar: number
  pedidos: PedidoDeliveryPersistido[]
}

export const consultarDelivery = (
  localId: string,
  pedidoIds: string[]
): Promise<SeguimientoDelivery> =>
  pedir<SeguimientoDelivery>(
    "POST",
    `${L(localId)}/delivery/consultar`,
    { cuerpo: { pedido_ids: pedidoIds } }
  ).then((r) => r.datos)
