import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Lightbulb, TrendingUp, Target, Users, Gift, AlertCircle } from "lucide-react";

interface ClusterData {
  cluster: number;
  num_clientes: number;
  porcentaje_clientes: number;
  promedios: {
    frecuencia: number;
    volumen_total: number;
    diversidad_productos: number;
    diversidad_categorias: number;
  };
}

interface BusinessRecommendationsProps {
  clusters: ClusterData[];
  title?: string;
}

const clusterNames = [
  "Ocasionales",
  "VIP",
  "Regulares",
  "Frecuentes"
];

const clusterDescriptions = [
  {
    nombre: "Ocasionales",
    descripcion: "Clientes con muy baja frecuencia de compra y tickets pequeños",
    icon: Users,
    color: "bg-muted"
  },
  {
    nombre: "VIP",
    descripcion: "Clientes súper heavy users con máxima frecuencia y volumen",
    icon: Gift,
    color: "bg-primary"
  },
  {
    nombre: "Regulares",
    descripcion: "Clientes con frecuencia moderada y volumen medio",
    icon: Target,
    color: "bg-secondary"
  },
  {
    nombre: "Frecuentes",
    descripcion: "Clientes frecuentes de alto valor con buena diversidad",
    icon: TrendingUp,
    color: "bg-accent"
  }
];

// Generar recomendaciones basadas en el perfil del cluster
const generateRecommendations = (cluster: ClusterData, index: number): {
  estrategia: string;
  acciones: string[];
  prioridad: "alta" | "media" | "baja";
} => {
  const { frecuencia, volumen_total, diversidad_productos, diversidad_categorias, porcentaje_clientes } = cluster;
  
  // Cluster 0: Ocasionales
  if (index === 0) {
    return {
      estrategia: "Reactivación y Fidelización",
      prioridad: "alta",
      acciones: [
        `Campañas de reactivación para ${porcentaje_clientes.toFixed(1)}% de la base (${cluster.num_clientes.toLocaleString()} clientes)`,
        "Programas de bienvenida con descuentos en segunda compra",
        "Email marketing con ofertas personalizadas según historial",
        "Cupones de descuento para incentivar compras más frecuentes",
        "Análisis de razones de abandono (encuestas post-compra)",
        "Programas de referidos: incentivos por traer nuevos clientes"
      ]
    };
  }
  
  // Cluster 1: VIP
  if (index === 1) {
    return {
      estrategia: "Retención y Exclusividad",
      prioridad: "alta",
      acciones: [
        `Programa VIP exclusivo para ${porcentaje_clientes.toFixed(1)}% de clientes más valiosos`,
        "Beneficios premium: envío gratis, acceso anticipado a ofertas",
        "Personalización avanzada: recomendaciones basadas en su alta diversidad",
        "Programa de puntos con recompensas de mayor valor",
        "Eventos exclusivos y experiencias personalizadas",
        "Asignar ejecutivo de cuenta para atención personalizada"
      ]
    };
  }
  
  // Cluster 2: Regulares
  if (index === 2) {
    return {
      estrategia: "Crecimiento y Engagement",
      prioridad: "media",
      acciones: [
        `Programas de fidelización para ${porcentaje_clientes.toFixed(1)}% de clientes regulares`,
        "Incentivos para aumentar frecuencia: compra 5, obtén descuento en la 6ta",
        "Cross-selling: sugerir productos complementarios según categorías compradas",
        "Newsletter con contenido relevante y ofertas segmentadas",
        "Programa de puntos estándar con beneficios progresivos",
        "Recordatorios inteligentes basados en patrones de compra"
      ]
    };
  }
  
  // Cluster 3: Frecuentes
  if (index === 3) {
    return {
      estrategia: "Optimización y Upselling",
      prioridad: "media",
      acciones: [
        `Estrategias de upselling para ${porcentaje_clientes.toFixed(1)}% de clientes frecuentes`,
        "Recomendaciones de productos premium en categorías que ya compran",
        "Ofertas de paquetes y combos para aumentar ticket promedio",
        "Programa de lealtad con beneficios escalonados",
        "Análisis de canasta promedio para sugerir productos adicionales",
        "Comunicación proactiva sobre nuevos productos en sus categorías favoritas"
      ]
    };
  }
  
  return {
    estrategia: "Segmentación General",
    prioridad: "baja",
    acciones: ["Análisis continuo del comportamiento", "Ajuste de estrategias según resultados"]
  };
};

// Calcular valor estimado del cluster
const calculateClusterValue = (cluster: ClusterData): number => {
  // Valor estimado = frecuencia * volumen * diversidad (normalizado)
  const valor = cluster.promedios.frecuencia * 
                cluster.promedios.volumen_total * 
                (cluster.promedios.diversidad_productos / 100) *
                cluster.num_clientes;
  return valor;
};

export const BusinessRecommendations = ({ 
  clusters,
  title = "Recomendaciones de Negocio por Segmento" 
}: BusinessRecommendationsProps) => {
  if (!clusters || clusters.length === 0) {
    return (
      <Card className="animate-slide-up">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Lightbulb className="h-5 w-5 text-primary" />
            {title}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-center py-8">
            No hay datos de segmentación disponibles
          </p>
        </CardContent>
      </Card>
    );
  }

  // Ordenar clusters por valor estimado (de mayor a menor)
  const sortedClusters = [...clusters].map((cluster, index) => ({
    ...cluster,
    index,
    valor: calculateClusterValue(cluster),
    recomendaciones: generateRecommendations(cluster, index)
  })).sort((a, b) => b.valor - a.valor);

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Lightbulb className="h-5 w-5 text-primary" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-6">
          {/* Resumen ejecutivo */}
          <div className="p-4 bg-primary/5 rounded-lg border border-primary/20">
            <h3 className="font-semibold text-lg mb-2 flex items-center gap-2">
              <AlertCircle className="h-5 w-5 text-primary" />
              Resumen Ejecutivo
            </h3>
            <p className="text-sm text-muted-foreground">
              Basado en el análisis de {clusters.reduce((sum, c) => sum + c.num_clientes, 0).toLocaleString()} clientes segmentados, 
              se identifican {clusters.length} grupos con comportamientos distintos. 
              El segmento VIP representa solo el {clusters[1]?.porcentaje_clientes.toFixed(1)}% de los clientes 
              pero genera el mayor valor por cliente. Los clientes ocasionales ({clusters[0]?.porcentaje_clientes.toFixed(1)}%) 
              representan la mayor oportunidad de crecimiento.
            </p>
          </div>

          {/* Recomendaciones por segmento */}
          {sortedClusters.map((clusterData, idx) => {
            const clusterInfo = clusterDescriptions[clusterData.index];
            const Icon = clusterInfo.icon;
            const recomendaciones = clusterData.recomendaciones;

            return (
              <div 
                key={clusterData.cluster} 
                className="border rounded-lg p-4 space-y-3 hover:shadow-md transition-shadow"
              >
                {/* Header del segmento */}
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className={`p-2 rounded-lg ${clusterInfo.color} text-white`}>
                      <Icon className="h-5 w-5" />
                    </div>
                    <div>
                      <h3 className="font-semibold text-lg">
                        Segmento {clusterData.cluster}: {clusterInfo.nombre}
                      </h3>
                      <p className="text-sm text-muted-foreground">
                        {clusterInfo.descripcion}
                      </p>
                    </div>
                  </div>
                  <Badge 
                    variant={recomendaciones.prioridad === "alta" ? "destructive" : 
                            recomendaciones.prioridad === "media" ? "default" : "secondary"}
                  >
                    Prioridad {recomendaciones.prioridad}
                  </Badge>
                </div>

                {/* Métricas del segmento */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                  <div className="p-2 bg-muted/50 rounded">
                    <div className="text-xs text-muted-foreground">Clientes</div>
                    <div className="font-bold">{clusterData.num_clientes.toLocaleString()}</div>
                    <div className="text-xs text-muted-foreground">
                      ({clusterData.porcentaje_clientes.toFixed(1)}%)
                    </div>
                  </div>
                  <div className="p-2 bg-muted/50 rounded">
                    <div className="text-xs text-muted-foreground">Frecuencia</div>
                    <div className="font-bold">{clusterData.promedios.frecuencia.toFixed(1)}</div>
                    <div className="text-xs text-muted-foreground">compras</div>
                  </div>
                  <div className="p-2 bg-muted/50 rounded">
                    <div className="text-xs text-muted-foreground">Volumen</div>
                    <div className="font-bold">{clusterData.promedios.volumen_total.toFixed(0)}</div>
                    <div className="text-xs text-muted-foreground">productos</div>
                  </div>
                  <div className="p-2 bg-muted/50 rounded">
                    <div className="text-xs text-muted-foreground">Diversidad</div>
                    <div className="font-bold">{clusterData.promedios.diversidad_productos.toFixed(0)}</div>
                    <div className="text-xs text-muted-foreground">productos únicos</div>
                  </div>
                </div>

                {/* Estrategia y acciones */}
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    <Target className="h-4 w-4 text-primary" />
                    <span className="font-medium text-sm">Estrategia: {recomendaciones.estrategia}</span>
                  </div>
                  <div className="pl-6 space-y-1">
                    <div className="text-xs font-medium text-muted-foreground mb-2">Acciones Recomendadas:</div>
                    <ul className="space-y-1.5">
                      {recomendaciones.acciones.map((accion, i) => (
                        <li key={i} className="text-sm flex items-start gap-2">
                          <span className="text-primary mt-1">•</span>
                          <span>{accion}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </div>
            );
          })}

          {/* Recomendaciones generales */}
          <div className="p-4 bg-accent/10 rounded-lg border border-accent/20">
            <h3 className="font-semibold text-lg mb-3 flex items-center gap-2">
              <TrendingUp className="h-5 w-5 text-accent" />
              Recomendaciones Generales
            </h3>
            <ul className="space-y-2 text-sm">
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span>
                  <strong>Implementar sistema de scoring:</strong> Asignar puntuación a cada cliente basada en 
                  frecuencia, volumen y diversidad para priorizar acciones.
                </span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span>
                  <strong>Automatizar comunicaciones:</strong> Usar triggers basados en comportamiento 
                  (ej: si un cliente ocasional no compra en 30 días, enviar oferta de reactivación).
                </span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span>
                  <strong>Monitoreo continuo:</strong> Re-evaluar segmentación cada trimestre para detectar 
                  migraciones entre segmentos y ajustar estrategias.
                </span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-accent mt-1">•</span>
                <span>
                  <strong>ROI por segmento:</strong> Medir efectividad de cada estrategia y optimizar 
                  inversión en marketing según retorno por segmento.
                </span>
              </li>
            </ul>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

