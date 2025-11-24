import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { TrendingUp } from "lucide-react";
import { useMemo } from "react";

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

interface CorrelationHeatmapProps {
  clusters: ClusterData[];
  featureNames?: string[];
  title?: string;
}

// Calcular correlación de Pearson entre dos variables
const calculateCorrelation = (
  values1: number[],
  values2: number[]
): number => {
  if (values1.length !== values2.length || values1.length === 0) {
    return 0;
  }

  const n = values1.length;
  const mean1 = values1.reduce((a, b) => a + b, 0) / n;
  const mean2 = values2.reduce((a, b) => a + b, 0) / n;

  let numerator = 0;
  let sumSq1 = 0;
  let sumSq2 = 0;

  for (let i = 0; i < n; i++) {
    const diff1 = values1[i] - mean1;
    const diff2 = values2[i] - mean2;
    numerator += diff1 * diff2;
    sumSq1 += diff1 * diff1;
    sumSq2 += diff2 * diff2;
  }

  const denominator = Math.sqrt(sumSq1 * sumSq2);
  if (denominator === 0) return 0;

  return numerator / denominator;
};

// Calcular matriz de correlación
const calculateCorrelationMatrix = (clusters: ClusterData[]): number[][] => {
  const variables = ['frecuencia', 'volumen_total', 'diversidad_productos', 'diversidad_categorias'] as const;
  
  // Extraer valores de cada variable por cluster
  const variableValues = variables.map(varName => 
    clusters.map(cluster => cluster.promedios[varName])
  );

  // Calcular matriz de correlación
  const matrix: number[][] = [];
  for (let i = 0; i < variables.length; i++) {
    const row: number[] = [];
    for (let j = 0; j < variables.length; j++) {
      if (i === j) {
        row.push(1.0); // Correlación consigo mismo es 1
      } else {
        const corr = calculateCorrelation(variableValues[i], variableValues[j]);
        row.push(corr);
      }
    }
    matrix.push(row);
  }

  return matrix;
};

// Obtener color según valor de correlación
const getCorrelationColor = (value: number): string => {
  if (value >= 0.7) return "hsl(var(--success))";
  if (value >= 0.3) return "hsl(var(--primary))";
  if (value >= -0.3) return "hsl(var(--muted))";
  if (value >= -0.7) return "hsl(var(--destructive))";
  return "hsl(var(--destructive))";
};

// Obtener opacidad según valor absoluto
const getOpacity = (value: number): number => {
  return Math.min(0.9, Math.abs(value) * 0.8 + 0.3);
};

export const CorrelationHeatmap = ({ 
  clusters,
  featureNames = ["Frecuencia", "Volumen Total", "Diversidad Productos", "Diversidad Categorías"],
  title = "Heatmap de Correlación entre Variables" 
}: CorrelationHeatmapProps) => {
  const correlationMatrix = useMemo(() => {
    if (!clusters || clusters.length === 0) return null;
    return calculateCorrelationMatrix(clusters);
  }, [clusters]);

  if (!correlationMatrix) {
    return (
      <Card className="animate-slide-up">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-primary" />
            {title}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-center py-8">
            No hay datos suficientes para calcular correlaciones
          </p>
        </CardContent>
      </Card>
    );
  }

  // Preparar datos para visualización
  const heatmapData: Array<{
    variable: string;
    [key: string]: string | number;
  }> = [];

  featureNames.forEach((name, i) => {
    const row: { variable: string; [key: string]: string | number } = {
      variable: name
    };
    featureNames.forEach((colName, j) => {
      row[colName] = correlationMatrix[i][j];
    });
    heatmapData.push(row);
  });

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <TrendingUp className="h-5 w-5 text-primary" />
          {title}
          <span className="text-sm font-normal text-muted-foreground ml-2">
            (Basado en promedios por cluster)
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {/* Matriz de correlación visual */}
          <div className="overflow-x-auto">
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  <th className="text-xs font-medium text-muted-foreground p-2 text-left border border-border">
                    Variable
                  </th>
                  {featureNames.map((name) => (
                    <th
                      key={name}
                      className="text-xs font-medium text-muted-foreground p-2 text-center border border-border min-w-[120px]"
                    >
                      {name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {heatmapData.map((row, i) => (
                  <tr key={row.variable}>
                    <td className="text-xs font-medium p-2 border border-border bg-muted/30">
                      {row.variable}
                    </td>
                    {featureNames.map((colName, j) => {
                      const value = correlationMatrix[i][j];
                      const color = getCorrelationColor(value);
                      const opacity = getOpacity(value);
                      const isDiagonal = i === j;

                      return (
                        <td
                          key={`${i}-${j}`}
                          className="p-2 border border-border text-center relative"
                        >
                          <div
                            className="w-full h-12 rounded flex items-center justify-center text-xs font-semibold transition-all hover:scale-105"
                            style={{
                              backgroundColor: isDiagonal
                                ? "hsl(var(--muted))"
                                : `${color}${Math.round(opacity * 255).toString(16).padStart(2, '0')}`,
                              color: isDiagonal
                                ? "hsl(var(--muted-foreground))"
                                : Math.abs(value) > 0.5
                                ? "white"
                                : "hsl(var(--foreground))",
                            }}
                            title={`Correlación: ${value.toFixed(3)}`}
                          >
                            {isDiagonal ? "1.000" : value.toFixed(3)}
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Leyenda */}
          <div className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 rounded" style={{ backgroundColor: "hsl(var(--success))" }}></div>
              <span>Correlación fuerte positiva (≥0.7)</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 rounded" style={{ backgroundColor: "hsl(var(--primary))" }}></div>
              <span>Correlación moderada (0.3-0.7)</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 rounded bg-muted"></div>
              <span>Correlación débil (-0.3 a 0.3)</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 rounded" style={{ backgroundColor: "hsl(var(--destructive))" }}></div>
              <span>Correlación negativa (≤-0.3)</span>
            </div>
          </div>

          {/* Interpretación */}
          <div className="p-4 bg-muted/50 rounded-lg text-sm">
            <p className="font-medium mb-2">Interpretación:</p>
            <ul className="list-disc list-inside space-y-1 text-muted-foreground">
              <li>
                <strong>Correlación positiva alta:</strong> Las variables aumentan juntas
              </li>
              <li>
                <strong>Correlación negativa:</strong> Cuando una variable aumenta, la otra disminuye
              </li>
              <li>
                <strong>Cerca de 0:</strong> No hay relación lineal significativa
              </li>
              <li>
                <strong>Nota:</strong> Esta correlación se calcula usando los promedios por cluster de segmentación
              </li>
            </ul>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

