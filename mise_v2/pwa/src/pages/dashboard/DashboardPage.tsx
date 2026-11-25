import { useEffect, useState } from "react"
import { Link } from "react-router-dom"
import { turnoActual } from "@/api/caja"
import { importarDelivery, estadoDelivery, type EstadoDelivery, type ResultadoImportacion } from "@/api/delivery"
import type { Turno } from "@/api/tipos"
import { dia, type ResumenDia } from "@/api/reportes"
import { Aviso } from "@/components/comun/Aviso"
import { Tarjeta } from "@/components/dashboard/ui"
import { mensajeDeError } from "@/hooks/useComando"
import { pesos } from "@/lib/dinero"
import { fechaHora, hoySantiago } from "@/lib/fechas"
import { useSesion } from "@/sesion/useSesion"

const ACCESOS = [
  ["catalogo", "Catálogo", "Productos, precios y modificadores"],
  ["salon", "Salón y cocina", "Sectores, mesas, cuartos e impresoras"],
  ["inventario", "Inventario", "Materias primas y recetas"],
  ["personal", "Personal", "Personas, roles e invitaciones"],
  ["configuracion", "Configuración", "Local, estaciones y código de autorización"],
  ["reportes", "Reportes", "Turnos y resumen por día"],
] as const

const CANTIDADES = [10, 50, 100, 500]
const ICONOS: Record<string, string> = { UBEREATS: "🟢", RAPPI: "🟠", PEDIDOSYA: "🔴" }

export default function DashboardPage() {
  const { local } = useSesion()
  const localId = local!.local_id
  const [turno, setTurno] = useState<Turno | null | undefined>(undefined)
  const [resumen, setResumen] = useState<ResumenDia | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Estado del panel de delivery
  const [streamEstado, setStreamEstado] = useState<EstadoDelivery | null>(null)
  const [cantidad, setCantidad] = useState(50)
  const [importando, setImportando] = useState(false)
  const [resultado, setResultado] = useState<ResultadoImportacion | null>(null)
  const [errorDelivery, setErrorDelivery] = useState<string | null>(null)

  useEffect(() => {
    turnoActual(localId).then(setTurno, (e: unknown) => setError(mensajeDeError(e)))
    dia(localId, hoySantiago()).then(setResumen, () => setResumen(null))
    estadoDelivery(localId).then(setStreamEstado).catch(() => setStreamEstado(null))
  }, [localId])

  const handleImportar = async () => {
    setImportando(true)
    setResultado(null)
    setErrorDelivery(null)
    try {
      const res = await importarDelivery(localId, cantidad)
      setResultado(res)
    } catch (e: unknown) {
      setErrorDelivery(mensajeDeError(e))
    } finally {
      setImportando(false)
    }
  }

  const r = resumen?.resumen
  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold tracking-tight">Inicio</h1>
      {error && <Aviso tipo="error">{error}</Aviso>}
      <div className="grid gap-4 md:grid-cols-2">
        <Tarjeta titulo="Turno actual">
          {turno === undefined ? <p className="text-sm text-neutral-500">Cargando…</p>
            : turno === null ? <p className="text-sm text-neutral-600">Sin turno abierto.</p>
            : <p className="text-sm">Abierto desde {fechaHora(turno.abierto_en)} · apertura {pesos(turno.monto_apertura)}</p>}
        </Tarjeta>
        <Tarjeta titulo={`Resumen de hoy (${hoySantiago()})`}>
          <dl className="grid grid-cols-2 gap-1 text-sm">
            <dt>Ventas</dt><dd className="text-right">{r?.n_ventas ?? 0}</dd>
            <dt>Total vendido</dt><dd className="text-right">{pesos(r?.total_ventas ?? 0)}</dd>
            <dt>Propinas</dt><dd className="text-right">{pesos(r?.total_propinas ?? 0)}</dd>
            <dt>Mesas cerradas sin venta</dt><dd className="text-right">{r?.n_cerrados_sin_venta ?? 0}</dd>
          </dl>
        </Tarjeta>
      </div>

      {/* ── Panel Importar Datos de Apps de Delivery (Kinesis) ── */}
      <Tarjeta titulo="📦 Importar datos de apps de delivery">
        <div className="space-y-4">

          {/* Estado del stream */}
          <div className="flex items-center gap-2 text-sm">
            <span className={`inline-block h-2.5 w-2.5 rounded-full ${streamEstado?.activo ? "bg-green-500" : "bg-red-400"}`} />
            {streamEstado === null
              ? <span className="text-neutral-500">Verificando stream…</span>
              : streamEstado.activo
                ? <span className="text-green-700 font-medium">
                    Kinesis <code className="bg-neutral-100 px-1 rounded">{streamEstado.stream}</code> activo
                    · {streamEstado.shards} shard{streamEstado.shards !== 1 ? "s" : ""}
                    · retención {streamEstado.retencion_horas}h
                  </span>
                : <span className="text-red-600">{streamEstado.mensaje ?? "Stream no disponible"}</span>
            }
          </div>

          <p className="text-sm text-neutral-600">
            Genera pedidos simulados de <span className="font-medium">🟢 Uber Eats</span>,{" "}
            <span className="font-medium">🟠 Rappi</span> y{" "}
            <span className="font-medium">🔴 PedidosYa</span> y los envía al stream de Amazon Kinesis,
            donde el consumidor Lambda los escribe en DynamoDB.
          </p>

          {/* Selector de cantidad + botón */}
          <div className="flex flex-wrap items-center gap-3">
            <label className="text-sm font-medium text-neutral-700">Cantidad de pedidos:</label>
            <div className="flex gap-1">
              {CANTIDADES.map((n) => (
                <button
                  key={n}
                  onClick={() => setCantidad(n)}
                  className={`rounded-lg border px-3 py-1 text-sm transition ${
                    cantidad === n
                      ? "border-teal-600 bg-teal-600 text-white"
                      : "border-neutral-300 bg-white text-neutral-700 hover:border-teal-400"
                  }`}
                >
                  {n}
                </button>
              ))}
            </div>
            <button
              onClick={handleImportar}
              disabled={importando || !streamEstado?.activo}
              className="rounded-xl bg-teal-600 px-5 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-teal-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {importando ? "Enviando a Kinesis…" : "⬆ Importar datos de delivery"}
            </button>
          </div>

          {/* Resultado de la importación */}
          {resultado && (
            <div className="rounded-xl border border-green-200 bg-green-50 p-4 space-y-1">
              <p className="text-sm font-semibold text-green-800">✅ {resultado.mensaje}</p>
              <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-sm text-green-700 mt-1">
                <span>Enviados al stream</span><span className="font-mono font-bold">{resultado.enviados}</span>
                <span>Fallidos</span><span className="font-mono">{resultado.fallidos}</span>
                <span>Duración</span><span className="font-mono">{resultado.duracion_ms} ms</span>
                <span>Plataformas</span>
                <span>{resultado.plataformas.map((p) => `${ICONOS[p] ?? ""} ${p}`).join(" · ")}</span>
              </div>
            </div>
          )}

          {errorDelivery && <Aviso tipo="error">{errorDelivery}</Aviso>}
        </div>
      </Tarjeta>

      {/* ── Accesos rápidos ── */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {ACCESOS.map(([ruta, nombre, descripcion]) => (
          <Link key={ruta} to={`/l/${localId}/admin/${ruta}`} className="rounded-2xl border bg-white p-4 transition hover:shadow-md">
            <p className="font-semibold">{nombre}</p>
            <p className="text-sm text-neutral-500">{descripcion}</p>
          </Link>
        ))}
      </div>
    </div>
  )
}

