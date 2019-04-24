<?php

namespace bashkarev\clickhouse;


use yii\db\ArrayExpression;
use yii\db\ExpressionBuilderInterface;
use yii\db\ExpressionBuilderTrait;
use yii\db\ExpressionInterface;
use yii\db\Query;
use Traversable;

class ArrayExpressionBuilder implements ExpressionBuilderInterface
{
    use ExpressionBuilderTrait;

    /**
     * {@inheritdoc}
     * @param ArrayExpression|ExpressionInterface $expression the expression to be built
     */
    public function build(ExpressionInterface $expression, array &$params = []): string
    {
        $value = $expression->getValue();
        if ($value === null) {
            return 'NULL';
        }

        if ($value instanceof Query) {
            list ($sql, $params) = $this->queryBuilder->build($value, $params);
            return $this->buildSubqueryArray($sql);
        }

        $placeholders = $this->buildPlaceholders($expression, $params);

        return '[' . implode(', ', $placeholders) . ']';
    }

    /**
     * Builds placeholders array out of $expression values
     * @param ExpressionInterface|ArrayExpression $expression
     * @param array $params the binding parameters.
     * @return array
     */
    protected function buildPlaceholders(ExpressionInterface $expression, &$params): array
    {
        $value = $expression->getValue();

        $placeholders = [];
        if ($value === null || (!is_array($value) && !$value instanceof Traversable)) {
            return $placeholders;
        }

        if ($expression->getDimension() > 1) {
            foreach ($value as $item) {
                $placeholders[] = $this->build($this->unnestArrayExpression($expression, $item), $params);
            }
            return $placeholders;
        }

        foreach ($value as $item) {
            if ($item instanceof Query) {
                list ($sql, $params) = $this->queryBuilder->build($item, $params);
                $placeholders[] = $this->buildSubqueryArray($sql);
                continue;
            }

            if ($item instanceof ExpressionInterface) {
                $placeholders[] = $this->queryBuilder->buildExpression($item, $params);
                continue;
            }

            $placeholders[] = $this->queryBuilder->bindParam($item, $params);
        }

        return $placeholders;
    }

    private function unnestArrayExpression(ArrayExpression $expression, $value): ArrayExpression
    {
        $expressionClass = get_class($expression);

        return new $expressionClass($value, $expression->getType(), $expression->getDimension() - 1);
    }

    protected function buildSubqueryArray($sql): string
    {
        return "array({$sql})";
    }
}
