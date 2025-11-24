import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { Calendar } from "lucide-react";

interface WeeklyData {
  dia_semana: string;
  num_transacciones: number;
  porcentaje: number;
}

interface WeeklyDistributionChartProps {
  data: WeeklyData[];
  title?: string;
}

const dayColors = [
  "hsl(var(--accent))",      // Domingo
  "hsl(var(--primary))",     // Lunes
  "hsl(var(--secondary))",   // Martes
  "hsl(var(--success))",     // Miércoles
  "hsl(var(--accent))",      // Jueves
  "hsl(var(--primary))",     // Viernes
  "hsl(var(--secondary))",   // Sábado
];

export const WeeklyDistributionChart = ({ 
  data, 
  title = "Distribución de Transacciones por Día" 
}: WeeklyDistributionChartProps) => {
  const chartData = data.map(item => ({
    dia: item.dia_semana.substring(0, 3),
    transacciones: item.num_transacciones,
    porcentaje: item.porcentaje,
  }));

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Calendar className="h-5 w-5 text-accent" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <defs>
              {dayColors.map((color, index) => (
                <linearGradient key={index} id={`gradient-${index}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={color} stopOpacity={0.9}/>
                  <stop offset="95%" stopColor={color} stopOpacity={0.6}/>
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="dia" 
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
                `${value.toLocaleString()} (${props.payload.porcentaje}%)`,
                'Transacciones'
              ]}
            />
            <Legend />
            <Bar 
              dataKey="transacciones" 
              radius={[8, 8, 0, 0]}
              animationDuration={800}
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={`url(#gradient-${index})`} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};
