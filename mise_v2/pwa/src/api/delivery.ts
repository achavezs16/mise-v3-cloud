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
