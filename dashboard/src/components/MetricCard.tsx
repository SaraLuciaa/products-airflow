import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: LucideIcon;
  variant?: "default" | "primary" | "secondary" | "accent";
  className?: string;
}

export const MetricCard = ({
  title,
  value,
  subtitle,
  icon: Icon,
  variant = "default",
  className,
}: MetricCardProps) => {
  const variantStyles = {
    default: "border-border",
    primary: "border-primary/20 bg-gradient-to-br from-primary/5 to-primary/10",
    secondary: "border-secondary/20 bg-gradient-to-br from-secondary/5 to-secondary/10",
    accent: "border-accent/20 bg-gradient-to-br from-accent/5 to-accent/10",
  };

  const iconVariantStyles = {
    default: "bg-muted text-foreground",
    primary: "bg-primary text-primary-foreground",
    secondary: "bg-secondary text-secondary-foreground",
    accent: "bg-accent text-accent-foreground",
  };

  return (
    <Card className={cn("animate-fade-in hover:shadow-lg transition-all duration-300", variantStyles[variant], className)}>
      <CardHeader className="flex flex-row items-center justify-between pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">
          {title}
        </CardTitle>
        <div className={cn("p-2 rounded-lg", iconVariantStyles[variant])}>
          <Icon className="h-4 w-4" />
        </div>
      </CardHeader>
      <CardContent>
        <div className="text-3xl font-bold">{value}</div>
        {subtitle && (
          <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>
        )}
      </CardContent>
    </Card>
  );
};
