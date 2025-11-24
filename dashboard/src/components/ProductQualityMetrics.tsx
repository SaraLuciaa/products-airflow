import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { MetricCard } from "@/components/MetricCard";
import { Database, AlertCircle, CheckCircle2, XCircle } from "lucide-react";

interface ProductQualityData {
  Categories: {
    num_filas: number;
    num_columnas: number;
    total_nulos: number;
    num_duplicados: number;
    porcentaje_duplicados: number;
  };
  ProductCategory: {
    num_filas: number;
    num_columnas: number;
    total_nulos: number;
    num_duplicados: number;
    porcentaje_duplicados: number;
  };
}

interface ProductQualityMetricsProps {
  data: ProductQualityData;
  title?: string;
}

export const ProductQualityMetrics = ({ 
  data, 
  title = "Calidad de Datos de Productos" 
}: ProductQualityMetricsProps) => {
  const categories = data.Categories;
  const productCategory = data.ProductCategory;

  return (
    <div className="space-y-6">
      <Card className="animate-slide-up">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="h-5 w-5 text-primary" />
            {title}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Categories Metrics */}
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground">Categories.csv</h3>
              <div className="grid grid-cols-2 gap-4">
                <MetricCard
                  title="Total Registros"
                  value={categories.num_filas.toLocaleString()}
                  subtitle={`${categories.num_columnas} columnas`}
                  icon={Database}
                  variant="primary"
                />
                <MetricCard
                  title="Valores Nulos"
                  value={categories.total_nulos}
                  subtitle={categories.total_nulos === 0 ? "Sin nulos" : "Requiere atención"}
                  icon={categories.total_nulos === 0 ? CheckCircle2 : AlertCircle}
                  variant={categories.total_nulos === 0 ? "default" : "accent"}
                />
                <MetricCard
                  title="Duplicados"
                  value={categories.num_duplicados}
                  subtitle={`${categories.porcentaje_duplicados}% del total`}
                  icon={categories.num_duplicados === 0 ? CheckCircle2 : XCircle}
                  variant={categories.num_duplicados === 0 ? "default" : "secondary"}
                />
              </div>
            </div>

            {/* ProductCategory Metrics */}
            <div className="space-y-4">
              <h3 className="text-lg font-semibold text-foreground">ProductCategory.csv</h3>
              <div className="grid grid-cols-2 gap-4">
                <MetricCard
                  title="Total Registros"
                  value={productCategory.num_filas.toLocaleString()}
                  subtitle={`${productCategory.num_columnas} columnas`}
                  icon={Database}
                  variant="primary"
                />
                <MetricCard
                  title="Valores Nulos"
                  value={productCategory.total_nulos}
                  subtitle={productCategory.total_nulos === 0 ? "Sin nulos" : "Requiere atención"}
                  icon={productCategory.total_nulos === 0 ? CheckCircle2 : AlertCircle}
                  variant={productCategory.total_nulos === 0 ? "default" : "accent"}
                />
                <MetricCard
                  title="Duplicados"
                  value={productCategory.num_duplicados.toLocaleString()}
                  subtitle={`${productCategory.porcentaje_duplicados}% del total`}
                  icon={productCategory.num_duplicados === 0 ? CheckCircle2 : XCircle}
                  variant={productCategory.num_duplicados === 0 ? "default" : "secondary"}
                />
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};


