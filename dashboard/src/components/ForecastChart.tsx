import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { TrendingUp } from "lucide-react";

interface ForecastData {
  month: string;
  yhat: number;
  store_id?: number;
}

interface ForecastChartProps {
  data: ForecastData[];
  title?: string;
  storeId?: number;
}

export const ForecastChart = ({ data, title = "Pronóstico Mensual", storeId }: ForecastChartProps) => {
  const chartData = data
    .filter(item => !storeId || item.store_id === storeId)
    .map(item => ({
      month: new Date(item.month).toLocaleDateString('es-CO', { month: 'short', year: '2-digit' }),
      ventas: Math.round(item.yhat),
    }));

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <TrendingUp className="h-5 w-5 text-accent" />
          {title}
          {storeId && <span className="text-sm font-normal text-muted-foreground">- Tienda {storeId}</span>}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={chartData}>
            <defs>
              <linearGradient id="colorVentas" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="hsl(var(--accent))" stopOpacity={0.8}/>
                <stop offset="95%" stopColor="hsl(var(--accent))" stopOpacity={0.1}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="month" 
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => value.toLocaleString()}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px',
              }}
              formatter={(value: number) => [value.toLocaleString(), 'Ventas Proyectadas']}
            />
            <Legend />
            <Area
              type="monotone"
              dataKey="ventas"
              stroke="hsl(var(--accent))"
              strokeWidth={3}
              fill="url(#colorVentas)"
              dot={{ fill: 'hsl(var(--accent))', strokeWidth: 2, r: 6, stroke: 'hsl(var(--card))' }}
              activeDot={{ r: 8, strokeWidth: 3 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};
