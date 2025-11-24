import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceLine } from "recharts";
import { Calendar } from "lucide-react";

interface DailyData {
  date: string;
  num_transacciones: number;
}

interface DailyTimeSeriesChartProps {
  data: DailyData[];
  title?: string;
}

export const DailyTimeSeriesChart = ({ 
  data, 
  title = "Serie de Tiempo Diaria - Transacciones" 
}: DailyTimeSeriesChartProps) => {
  // Calcular promedio para línea de referencia
  const promedio = data.length > 0 
    ? data.reduce((sum, item) => sum + item.num_transacciones, 0) / data.length 
    : 0;

  // Formatear datos para el gráfico
  const chartData = data.map(item => ({
    fecha: new Date(item.date).toLocaleDateString('es-CO', { 
      month: 'short', 
      day: 'numeric' 
    }),
    transacciones: item.num_transacciones,
    fechaCompleta: item.date
  }));

  // Encontrar picos (valores por encima del promedio + 1 desviación estándar)
  const desvStd = data.length > 0
    ? Math.sqrt(
        data.reduce((sum, item) => {
          const diff = item.num_transacciones - promedio;
          return sum + diff * diff;
        }, 0) / data.length
      )
    : 0;
  const umbralPico = promedio + desvStd;

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Calendar className="h-5 w-5 text-primary" />
          {title}
          <span className="text-sm font-normal text-muted-foreground ml-2">
            ({data.length} días)
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {/* Métricas resumen */}
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div className="text-center p-2 bg-muted/50 rounded">
              <div className="text-xs text-muted-foreground">Promedio Diario</div>
              <div className="font-bold text-lg">{Math.round(promedio).toLocaleString()}</div>
            </div>
            <div className="text-center p-2 bg-muted/50 rounded">
              <div className="text-xs text-muted-foreground">Máximo</div>
              <div className="font-bold text-lg">
                {Math.max(...data.map(d => d.num_transacciones)).toLocaleString()}
              </div>
            </div>
            <div className="text-center p-2 bg-muted/50 rounded">
              <div className="text-xs text-muted-foreground">Mínimo</div>
              <div className="font-bold text-lg">
                {Math.min(...data.map(d => d.num_transacciones)).toLocaleString()}
              </div>
            </div>
          </div>

          {/* Gráfico */}
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 60 }}>
              <defs>
                <linearGradient id="dailyGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.8}/>
                  <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0.1}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
              <XAxis 
                dataKey="fecha"
                stroke="hsl(var(--foreground))"
                style={{ fontSize: '10px' }}
                angle={-45}
                textAnchor="end"
                height={80}
                interval="preserveStartEnd"
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
                formatter={(value: number) => [
                  `${value.toLocaleString()} transacciones`,
                  'Transacciones'
                ]}
                labelFormatter={(label, payload) => {
                  const item = payload?.[0]?.payload;
                  return item?.fechaCompleta 
                    ? new Date(item.fechaCompleta).toLocaleDateString('es-CO', {
                        weekday: 'long',
                        year: 'numeric',
                        month: 'long',
                        day: 'numeric'
                      })
                    : label;
                }}
              />
              <Legend />
              <ReferenceLine 
                y={promedio} 
                stroke="hsl(var(--accent))" 
                strokeDasharray="5 5"
                label={{ value: `Promedio: ${Math.round(promedio).toLocaleString()}`, position: "topRight" }}
              />
              <Line
                type="monotone"
                dataKey="transacciones"
                stroke="hsl(var(--primary))"
                strokeWidth={2}
                dot={{ fill: 'hsl(var(--primary))', strokeWidth: 1, r: 3 }}
                activeDot={{ r: 6, strokeWidth: 2 }}
                fill="url(#dailyGradient)"
              />
            </LineChart>
          </ResponsiveContainer>

          {/* Información adicional */}
          <div className="text-xs text-muted-foreground text-center">
            <p>
              Días pico: {data.filter(d => d.num_transacciones > umbralPico).length} días 
              con transacciones superiores a {Math.round(umbralPico).toLocaleString()} 
              (promedio + 1 desviación estándar)
            </p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};


