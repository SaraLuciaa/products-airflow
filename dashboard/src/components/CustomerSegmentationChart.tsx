import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { Users } from "lucide-react";

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

interface CustomerSegmentationChartProps {
  data: ClusterData[];
  totalClientes?: number;
  title?: string;
}

const clusterNames = [
  "Ocasionales",
  "VIP",
  "Regulares",
  "Frecuentes"
];

const clusterColors = [
  "hsl(var(--muted))",
  "hsl(var(--primary))",
  "hsl(var(--secondary))",
  "hsl(var(--accent))",
];

export const CustomerSegmentationChart = ({ 
  data, 
  totalClientes,
  title = "Segmentación de Clientes (K-Means)" 
}: CustomerSegmentationChartProps) => {
  const chartData = data.map((item, index) => ({
    segmento: clusterNames[index] || `Cluster ${item.cluster}`,
    clientes: item.num_clientes,
    porcentaje: item.porcentaje_clientes,
    frecuencia: item.promedios.frecuencia,
    volumen: item.promedios.volumen_total,
    diversidad_prod: item.promedios.diversidad_productos,
    diversidad_cat: item.promedios.diversidad_categorias,
    color: clusterColors[index % clusterColors.length]
  }));

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Users className="h-5 w-5 text-primary" />
          {title}
          {totalClientes && (
            <span className="text-sm font-normal text-muted-foreground ml-2">
              ({totalClientes.toLocaleString()} clientes totales)
            </span>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-6">
          {/* Distribution Chart */}
          <div>
            <h3 className="text-sm font-medium text-muted-foreground mb-4">Distribución de Clientes por Segmento</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                <XAxis 
                  dataKey="segmento"
                  stroke="hsl(var(--foreground))"
                  style={{ fontSize: '12px' }}
                />
                <YAxis 
                  stroke="hsl(var(--foreground))"
                  style={{ fontSize: '12px' }}
                  tickFormatter={(value) => `${(value / 1000).toFixed(0)}K`}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(var(--card))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                  }}
                  formatter={(value: number, name: string, props: any) => [
                    `${value.toLocaleString()} clientes (${props.payload.porcentaje.toFixed(2)}%)`,
                    'Clientes'
                  ]}
                />
                <Legend />
                <Bar 
                  dataKey="clientes" 
                  radius={[8, 8, 0, 0]}
                  animationDuration={800}
                >
                  {chartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Metrics Table */}
          <div>
            <h3 className="text-sm font-medium text-muted-foreground mb-4">Características por Segmento</h3>
            <div className="overflow-x-auto">
              <table className="w-full border-collapse">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-xs font-medium text-muted-foreground p-2 text-left">Segmento</th>
                    <th className="text-xs font-medium text-muted-foreground p-2 text-center">Clientes</th>
                    <th className="text-xs font-medium text-muted-foreground p-2 text-center">Frecuencia</th>
                    <th className="text-xs font-medium text-muted-foreground p-2 text-center">Volumen</th>
                    <th className="text-xs font-medium text-muted-foreground p-2 text-center">Diversidad Prod.</th>
                    <th className="text-xs font-medium text-muted-foreground p-2 text-center">Diversidad Cat.</th>
                  </tr>
                </thead>
                <tbody>
                  {chartData.map((item, index) => (
                    <tr key={index} className="border-b border-border hover:bg-muted/50 transition-colors">
                      <td className="p-2">
                        <div className="flex items-center gap-2">
                          <div 
                            className="w-3 h-3 rounded-full" 
                            style={{ backgroundColor: item.color }}
                          />
                          <span className="text-sm font-medium">{item.segmento}</span>
                        </div>
                      </td>
                      <td className="p-2 text-center text-sm">
                        {item.clientes.toLocaleString()}
                        <span className="text-xs text-muted-foreground ml-1">
                          ({item.porcentaje.toFixed(1)}%)
                        </span>
                      </td>
                      <td className="p-2 text-center text-sm">{item.frecuencia.toFixed(1)}</td>
                      <td className="p-2 text-center text-sm">{item.volumen.toFixed(1)}</td>
                      <td className="p-2 text-center text-sm">{item.diversidad_prod.toFixed(1)}</td>
                      <td className="p-2 text-center text-sm">{item.diversidad_cat.toFixed(1)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};


