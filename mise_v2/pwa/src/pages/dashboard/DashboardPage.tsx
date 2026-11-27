import { useEffect, useState } from "react"
import { Link } from "react-router-dom"
import { turnoActual } from "@/api/caja"
import { importarDelivery, estadoDelivery, consultarDelivery, type EstadoDelivery, type ResultadoImportacion, type SeguimientoDelivery } from "@/api/delivery"
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
const PLATAFORMAS: Record<string, string> = {
  UBEREATS: "Uber Eats",
  RAPPI: "Rappi",
  PEDIDOSYA: "PedidosYa",
}

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
  const [seguimiento, setSeguimiento] = useState<SeguimientoDelivery | null>(null)
  const [verificando, setVerificando] = useState(false)
  const [filasVisibles, setFilasVisibles] = useState(0)
  const [avisoSeguimiento, setAvisoSeguimiento] = useState<string | null>(null)

  useEffect(() => {
    turnoActual(localId).then(setTurno, (e: unknown) => setError(mensajeDeError(e)))
    dia(localId, hoySantiago()).then(setResumen, () => setResumen(null))
    estadoDelivery(localId).then(setStreamEstado).catch(() => setStreamEstado(null))
  }, [localId])

  const handleImportar = async () => {
    setImportando(true)
    setResultado(null)
    setErrorDelivery(null)
    setSeguimiento(null)
    setFilasVisibles(0)
    setAvisoSeguimiento(null)
    setVerificando(false)
    try {
      const res = await importarDelivery(localId, cantidad)
      setResultado(res)
      setVerificando(res.pedido_ids.length > 0)
    } catch (e: unknown) {
      setErrorDelivery(mensajeDeError(e))
    } finally {
      setImportando(false)
    }
  }

  useEffect(() => {
    if (!resultado?.pedido_ids?.length) return

    const ids = resultado.pedido_ids
    let cancelado = false
    let temporizador: ReturnType<typeof setTimeout> | undefined
    const inicio = Date.now()

    const consultar = async () => {
      if (cancelado) return

      try {
        const datos = await consultarDelivery(localId, ids)
        if (cancelado) return

        setSeguimiento(datos)

        if (datos.pendientes === 0) {
          setVerificando(false)
          return
        }

        if (Date.now() - inicio >= 30000) {
          setAvisoSeguimiento("Tiempo de seguimiento agotado. Los pedidos pendientes podrían continuar procesándose.")
          setVerificando(false)
          return
        }

        temporizador = setTimeout(consultar, 1000)
      } catch (e: unknown) {
        if (cancelado) return
        setAvisoSeguimiento(mensajeDeError(e))
        setVerificando(false)
      }
    }

    temporizador = setTimeout(consultar, 0)

    return () => {
      cancelado = true
      if (temporizador !== undefined) clearTimeout(temporizador)
    }
  }, [resultado, localId])

  const totalConfirmados = seguimiento?.pedidos.length ?? 0
  const presentando = filasVisibles < totalConfirmados

  useEffect(() => {
    if (!presentando) return

    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      setFilasVisibles(totalConfirmados)
      return
    }

    const cantidadTotal = resultado?.enviados ?? 10
    const intervalo = cantidadTotal <= 10 ? 250 : 100
    const paso = Math.max(1, Math.ceil(cantidadTotal / 25))

    const temporizador = window.setInterval(() => {
      setFilasVisibles((actual) =>
        Math.min(actual + paso, totalConfirmados)
      )
    }, intervalo)

    return () => window.clearInterval(temporizador)
  }, [presentando, totalConfirmados, resultado])

  const persistidos = seguimiento?.persistidos ?? 0
  const pendientes = seguimiento?.pendientes ?? resultado?.enviados ?? 0
  const progreso = resultado?.enviados
    ? Math.round(persistidos / resultado.enviados * 100)
    : 0

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
      <Tarjeta titulo="📦 Importar pedidos de delivery">
        <div className="space-y-4">

          {/* Estado del stream */}
          <div className="flex items-center gap-2 text-sm">
            <span className={`inline-block h-2.5 w-2.5 rounded-full ${streamEstado?.activo ? "bg-green-500" : "bg-red-400"}`} />
            {streamEstado === null
              ? <span className="text-neutral-500">Verificando disponibilidad…</span>
              : streamEstado.activo
                ? <span className="text-green-700 font-medium">
                    Servicio de importación disponible
                  </span>
                : <span className="text-red-600">Servicio de importación no disponible</span>
            }
          </div>

          <p className="text-sm text-neutral-600">
            Importa y consulta pedidos simulados de <span className="font-medium">🟢 Uber Eats</span>,{" "}
            <span className="font-medium">🟠 Rappi</span> y{" "}
            <span className="font-medium">🔴 PedidosYa</span> desde un solo lugar.
          </p>

          {/* Selector de cantidad + botón */}
          <div className="flex flex-wrap items-center gap-3">
            <label className="text-sm font-medium text-neutral-700">Cantidad de pedidos:</label>
            <div className="flex gap-1">
              {CANTIDADES.map((n) => (
                <button
                  key={n}
                  onClick={() => setCantidad(n)}
                  disabled={importando || verificando || presentando}
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
              disabled={importando || verificando || presentando || !streamEstado?.activo}
              className="rounded-xl bg-teal-600 px-5 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-teal-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {importando ? "Enviando pedidos…"
                : verificando ? "Confirmando pedidos…"
                : presentando ? "Mostrando pedidos…"
                : "⬇ Importar pedidos"}
            </button>
          </div>

          {/* Monitor de streaming y persistencia */}
          {resultado && (
            <div className="space-y-4 rounded-xl border border-neutral-200 bg-neutral-50 p-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <p className="font-semibold text-neutral-800">
                  Seguimiento de importación
                </p>
                <span className="rounded-full bg-white px-3 py-1 text-xs font-medium text-neutral-600 border">
                  {verificando ? "Procesando"
                    : resultado.fallidos > 0 || pendientes > 0 ? "Confirmación parcial"
                    : "Completado"}
                </span>
              </div>

              <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                {[
                  ["Pedidos enviados", resultado.enviados, "text-teal-700"],
                  ["Pedidos registrados", persistidos, "text-green-700"],
                  ["En proceso", pendientes, "text-amber-600"],
                  ["Errores de envío", resultado.fallidos, "text-red-600"],
                ].map(([etiqueta, valor, color]) => (
                  <div key={etiqueta} className="rounded-xl border bg-white p-3">
                    <p className="text-xs text-neutral-500">{etiqueta}</p>
                    <p className={`mt-1 text-2xl font-bold tabular-nums ${color}`}>
                      {valor}
                    </p>
                  </div>
                ))}
              </div>

              <div className="space-y-2">
                <div className="flex justify-between text-sm font-medium">
                  <span>Progreso de importación</span>
                  <span>{progreso}%</span>
                </div>
                <div
                  className="h-3 overflow-hidden rounded-full bg-neutral-200"
                  role="progressbar"
                  aria-label="Progreso de importación"
                  aria-valuenow={progreso}
                  aria-valuemin={0}
                  aria-valuemax={100}
                >
                  <div
                    className="h-full rounded-full bg-teal-600 transition-all duration-300"
                    style={{ width: `${progreso}%` }}
                  />
                </div>
                <p className="text-xs text-neutral-500">
                  {persistidos} de {resultado.enviados} pedidos enviados registrados correctamente.
                </p>
              </div>

              <p className="text-xs text-neutral-600">
                Pedidos solicitados: {resultado.cantidad_solicitada}
              </p>

              {avisoSeguimiento && (
                <p className="rounded-lg bg-amber-50 p-3 text-sm text-amber-800">
                  {avisoSeguimiento}
                </p>
              )}

              <div className="space-y-2">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold">Pedidos registrados</h3>
                  <span className="text-xs text-neutral-500">
                    Mostrando {Math.min(filasVisibles, totalConfirmados)} de {totalConfirmados}
                  </span>
                </div>

                <div className="flex flex-wrap gap-2">
                  {Object.entries(ICONOS).map(([plataforma, icono]) => (
                    <span key={plataforma} className="rounded-lg border bg-white px-3 py-1 text-xs">
                      {icono} {PLATAFORMAS[plataforma]}: {seguimiento?.pedidos.filter(
                        (p) => p.plataforma.toUpperCase() === plataforma
                      ).length ?? 0}
                    </span>
                  ))}
                </div>

                {seguimiento && seguimiento.pedidos.length > 0 ? (
                  <div className="max-h-80 overflow-auto rounded-lg border bg-white">
                    <table className="w-full text-left text-xs">
                      <thead className="sticky top-0 bg-neutral-100">
                        <tr>
                          {["ID del pedido", "Plataforma", "Ítems", "Total", "Estado"].map((h) => (
                            <th key={h} className="whitespace-nowrap px-3 py-2 font-semibold">
                              {h}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {seguimiento.pedidos
                          .slice(Math.max(0, totalConfirmados - filasVisibles))
                          .map((pedido) => (
                          <tr key={pedido.pedido_id} className="border-t">
                            <td className="whitespace-nowrap px-3 py-2 font-mono">
                              {pedido.pedido_id}
                            </td>
                            <td className="whitespace-nowrap px-3 py-2">
                              {ICONOS[pedido.plataforma.toUpperCase()] ?? ""}{" "}
                              {PLATAFORMAS[pedido.plataforma.toUpperCase()] ?? pedido.plataforma}
                            </td>
                            <td className="px-3 py-2">{pedido.n_items}</td>
                            <td className="whitespace-nowrap px-3 py-2">
                              {pesos(pedido.total)}
                            </td>
                            <td className="px-3 py-2 font-medium text-green-700">
                              Registrado
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="rounded-lg border bg-white p-3 text-sm text-neutral-500">
                    Aún no hay pedidos registrados.
                  </p>
                )}

                <p className="text-right text-sm font-semibold">
                  Total de pedidos registrados: {pesos(
                    seguimiento?.pedidos.reduce((total, p) => total + p.total, 0) ?? 0
                  )}
                </p>
                <p className="text-xs text-neutral-500">
                  Pedidos simulados. Estos montos no corresponden a ventas realizadas.
                </p>
              </div>

              <details className="border-t pt-3 text-xs text-neutral-500">
                <summary className="cursor-pointer font-medium">
                  Detalles técnicos
                </summary>
                <dl className="mt-3 grid gap-2 sm:grid-cols-2">
                  <div>
                    <dt className="font-medium">Servicio de streaming</dt>
                    <dd>Amazon Kinesis</dd>
                  </div>
                  <div>
                    <dt className="font-medium">Stream</dt>
                    <dd>{resultado.stream}</dd>
                  </div>
                  <div>
                    <dt className="font-medium">Shards</dt>
                    <dd>{streamEstado?.shards ?? "—"}</dd>
                  </div>
                  <div>
                    <dt className="font-medium">Retención</dt>
                    <dd>{streamEstado?.retencion_horas ?? "—"} horas</dd>
                  </div>
                  <div>
                    <dt className="font-medium">Tiempo de envío</dt>
                    <dd>{resultado.duracion_ms} ms</dd>
                  </div>
                  <div>
                    <dt className="font-medium">Procesamiento y almacenamiento</dt>
                    <dd>AWS Lambda · Amazon DynamoDB</dd>
                  </div>
                </dl>
                <p className="mt-3">
                  Los indicadores reflejan confirmaciones reales.
                  La aparición progresiva de las filas es únicamente visual.
                </p>
              </details>
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

