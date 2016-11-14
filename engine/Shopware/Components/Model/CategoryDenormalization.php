<?php
/**
 * Shopware 5
 * Copyright (c) shopware AG
 *
 * According to our dual licensing model, this program can be used either
 * under the terms of the GNU Affero General Public License, version 3,
 * or under a proprietary license.
 *
 * The texts of the GNU Affero General Public License with an additional
 * permission and of our proprietary license can be found at and
 * in the LICENSE file you have received along with this program.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * "Shopware" is a registered trademark of shopware AG.
 * The licensing of the program under the AGPLv3 does not imply a
 * trademark license. Therefore any rights, title and interest in
 * our trademarks remain entirely with us.
 */

namespace Shopware\Components\Model;

use Doctrine\DBAL\Connection;

/**
 * CategoryDenormalization-Class
 *
 * This class contains various methods to maintain
 * the denormalized representation of the Article to Category assignments.
 *
 * The assignments between articles and categories are stored in s_articles_categories.
 * The table s_articles_categories_ro contains each assignment of s_articles_categories
 * plus additional assignments for each child category.
 *
 * Most write operations take place in s_articles_categories_ro.
 *
 * @category  Shopware
 * @package   Shopware\Components\Model
 * @copyright Copyright (c) shopware AG (http://www.shopware.de)
 */
class CategoryDenormalization
{
    /**
     * @var \PDO
     */
    protected $connection;

    /**
     * @var bool
     */
    protected $enableTransactions = true;

    /**
     * @param Connection $connection
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param  Connection $connection
     * @return CategoryDenormalization
     */
    public function setConnection($connection)
    {
        $this->connection = $connection;
        return $this;
    }

    /**
     * @return bool
     */
    public function transactionsEnabled()
    {
        return $this->enableTransactions;
    }

    public function enableTransactions()
    {
        $this->enableTransactions = true;
    }

    public function disableTransactions()
    {
        $this->enableTransactions = false;
    }

    /**
     * Returns an array of all categoryIds the given $id has as parent
     *
     * Example:
     * $id = 9
     *
     * <code>
     * Array
     * (
     *     [0] => 9
     *     [1] => 5
     *     [2] => 10
     *     [3] => 3
     * )
     * <code>
     *
     * @param  integer $id
     * @return array
     */
    public function getParentCategoryIds($id)
    {
        $stmt = $this->connection->prepare('SELECT id, parent FROM s_categories WHERE id = :id AND parent IS NOT NULL');
        $stmt->execute(array(':id' => $id));
        $parent = $stmt->fetch(\PDO::FETCH_ASSOC);
        if (!$parent) {
            return array();
        }

        $result = array($parent['id']);

        $parent = $this->getParentCategoryIds($parent['parent']);
        if ($parent) {
            $result = array_merge($result, $parent);
        }

        return $result;
    }

    /**
     * Returns count for paging rebuildCategoryPath()
     *
     * @param  int $categoryId
     * @return int
     */
    public function rebuildCategoryPathCount($categoryId = null)
    {
        if ($categoryId === null) {
            $sql = '
                SELECT COUNT(id)
                FROM s_categories
                WHERE parent IS NOT NULL
            ';

            $stmt = $this->connection->prepare($sql);
            $stmt->execute();
        } else {
            $sql = '
                SELECT COUNT(c.id)
                FROM  s_categories c
                WHERE c.path LIKE :categoryId
            ';

            $stmt = $this->connection->prepare($sql);
            $stmt->execute(array('categoryId' => '%|' . $categoryId . '|%'));
        }

        $count = $stmt->fetchColumn();

        return (int)$count;
    }

    /**
     * Sets path for child categories of given $categoryId
     *
     * @param  int $categoryId
     * @param  int $count
     * @param  int $offset
     * @return int
     */
    public function rebuildCategoryPath($categoryId = null, $count = null, $offset = 0)
    {
        $parameters = array();
        if ($categoryId === null) {
            $sql = '
                SELECT id, path
                FROM  s_categories
                WHERE parent IS NOT NULL
            ';
        } else {
            $sql = '
                SELECT id, path
                FROM  s_categories
                WHERE path LIKE :categoryPath
            ';

            $parameters = array(
                'categoryPath' => '%|' . $categoryId . '|%'
            );
        }

        if ($count !== null) {
            $sql = $this->limit($sql, $count, $offset);
        }

        $stmt = $this->connection->prepare($sql);
        $stmt->execute($parameters);

        $count = 0;

        $this->beginTransaction();

        while ($category = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $count += $this->rebuildPath($category['id'], $category['path']);
        }

        $this->commit();

        return $count;
    }

    /**
     * Rebuilds the path for a single category
     *
     * @param $categoryId
     * @param $categoryPath
     * @return int
     */
    public function rebuildPath($categoryId, $categoryPath = null)
    {
        $updateStmt = $this->connection->prepare('UPDATE s_categories SET path = :path WHERE id = :categoryId');

        $parents = $this->getParentCategoryIds($categoryId);
        array_shift($parents);

        if (empty($parents)) {
            $path = null;
        } else {
            $path = implode('|', $parents);
            $path = '|' . $path . '|';
        }

        if ($categoryPath != $path) {
            $updateStmt->execute(array(':path' => $path, ':categoryId' => $categoryId));
            return 1;
        }

        return 0;
    }

    /**
     * Rebuilds the path for a single category
     *
     * @param  int $categoryId
     * @return int
     */
    public function removeOldAssignmentsCount($categoryId)
    {
        return 1;
    }

    /**
     * Used for category movement.
     * If Category is moved to a new parentId this returns removes old connections
     *
     * @param  int $categoryId
     * @param  int $count
     * @param  int $offset
     * @return int
     */
    public function removeOldAssignments($categoryId, $count = null, $offset = 0)
    {
        $categoryIds = $this->getChildCategories($categoryId);

        $sql = '
            DELETE
            FROM s_articles_categories_ro
            WHERE parentCategoryId IN (:categoryIds)
        ';

        $stmt = $this->connection->executeQuery(
            $sql,
            [':categoryIds' => (array)$categoryIds],
            [':categoryIds' => Connection::PARAM_INT_ARRAY]
        );

        $count = $stmt->rowCount();

        $sql = '
            SELECT parent FROM s_categories WHERE id = :categoryId
        ';

        $parentId = $this->connection->fetchColumn(
            $sql,
            [':categoryId' => $categoryId]
        );

        $categoryIds = array_diff($this->getChildCategories($parentId), $categoryIds);

        $count -= $this->fixAssignment($categoryIds);

        return $count;
    }

    private function getChildCategories($categoryIds)
    {
        $sql = '
            SELECT c2.id
            FROM s_categories c, s_categories c2
            WHERE (c2.path LIKE ' . $this->concat($this->quote('%|'), 'c.id', $this->quote('|%')) . ' OR c2.id = c.id)
            AND c.id IN (?)
        ';

        $statement = $this->connection->executeQuery($sql,
            array((array)$categoryIds),
            array(Connection::PARAM_INT_ARRAY)
        );

        return $statement->fetchAll(\PDO::FETCH_COLUMN);
    }

    /**
     * Returns count for paging rebuildAssignmentsCount()
     *
     * @param  int $categoryId
     * @return int
     */
    public function rebuildAssignmentsCount($categoryId)
    {
        $sql = '
            SELECT COUNT(c.id)
            FROM  s_categories c
            INNER JOIN s_articles_categories ac ON ac.categoryID = c.id
            WHERE c.path LIKE ' . $this->concat($this->quote('%|'), ':categoryId', $this->quote('|%')) . ' OR c.id = :categoryId
            GROUP BY c.id
        ';

        $stmt = $this->connection->prepare($sql);
        $stmt->execute(array('categoryId' => $categoryId));

        return $stmt->fetchColumn();
    }

    /**
     * @param  int $categoryId
     * @param  int $count
     * @param  int $offset
     * @return int
     */
    public function rebuildAssignments($categoryId, $count = null, $offset = 0)
    {
        // Fetch affected categories
        $affectedCategoriesSql = '
            SELECT c.id
            FROM  s_categories c
            INNER JOIN s_articles_categories ac ON ac.categoryID = c.id
            WHERE c.path LIKE :categoryId
            GROUP BY c.id
        ';

        if ($count !== null) {
            $affectedCategoriesSql = $this->limit($affectedCategoriesSql, $count, $offset);
        }

        $stmt = $this->connection->prepare($affectedCategoriesSql);
        $stmt->execute(array('categoryId' => '%|' . $categoryId . '|%'));

        $affectedCategories = array(
            $categoryId
        );
        while ($row = $stmt->fetchColumn()) {
            $affectedCategories[] = $row;
        }

        $assignmentsSql = 'SELECT articleID, categoryID FROM `s_articles_categories` WHERE categoryID = :categoryId';
        $assignmentsStmt = $this->connection->prepare($assignmentsSql);

        $result = 0;

        $this->beginTransaction();
        foreach ($affectedCategories as $categoryId) {
            $assignmentsStmt->execute(array('categoryId' => $categoryId));

            while ($assignment = $assignmentsStmt->fetch()) {
                $result += $this->addAssignment($assignment['articleID'], $assignment['categoryID']);
            }
        }
        $this->commit();

        return $result;
    }

    /**
     * Returns maxcount for paging rebuildAllAssignmentsCount()
     *
     * @return int
     */
    public function rebuildAllAssignmentsCount()
    {
        $sql = '
            SELECT COUNT(*) FROM s_articles_categories
        ';
        $stmt = $this->connection->query($sql);
        return (int)$stmt->fetchColumn();
    }

    /**
     * @param  int $count maximum number of assignments to denormalize
     * @param  int $offset
     * @return int number of new denormalized assignments
     */
    public function rebuildAllAssignments($count = null, $offset = 0)
    {
        $sql = '
            INSERT INTO s_articles_categories_ro (articleID, categoryID, parentCategoryID)

            SELECT ac.articleID, c2.id AS categoryID, c.id AS parentCategoryID
            FROM (' . $this->limit('SELECT articleID, categoryID FROM s_articles_categories', $count, $offset) . ') ac
            JOIN s_categories c
            ON ac.categoryID = c.id
            JOIN s_categories c2
            ON (c.path LIKE ' . $this->concat($this->quote('%|'), 'c2.id', $this->quote('|%')) . ' OR c2.id = c.id)
            LEFT JOIN s_articles_categories_ro ro
            ON ro.categoryID = c2.id AND ro.articleID = ac.articleID
            WHERE ro.id IS NULL
            ORDER BY ac.articleID, c2.id, c.id;
        ';

        return $this->connection->exec($sql);
    }

    /**
     * Removes assignments in s_articles_categories_ro
     *
     * @param  int $articleId
     * @param  int|array $categoryIds
     * @return int
     */
    public function removeAssignment($articleId, $categoryIds)
    {
        $sql = '
            DELETE FROM s_articles_categories_ro
            WHERE parentCategoryID IN (:categoryIds)
            AND articleID = :articleId
        ';
        $stmt = $this->connection->executeQuery(
            $sql,
            [
                ':articleId' => $articleId,
                ':categoryIds' => (array)$categoryIds
            ],
            [
                ':articleId' => \PDO::PARAM_INT,
                ':categoryIds' => Connection::PARAM_INT_ARRAY
            ]
        );

        $count = $stmt->rowCount();

        $count -= $this->fixAssignment($categoryIds, $articleId);

        return $count;
    }

    private function fixAssignment($categoryIds, $articleId = null)
    {
        if ($articleId == null) {
            $sql = ':articleId IS NULL AND ac.categoryID IN (:categoryIds)';
        } else {
            $sql = 'ac.articleID = :articleId AND ac.categoryID NOT IN (:categoryIds)';
        }
        $sql = '
            INSERT INTO s_articles_categories_ro (articleID, categoryID, parentCategoryID)

            SELECT ac.articleID, c2.id AS categoryID, c.id AS parentCategoryID
            FROM s_articles_categories ac
            JOIN s_categories c
            ON ac.categoryID = c.id
            JOIN s_categories c2
            ON (c.path LIKE ' . $this->concat($this->quote('%|'), 'c2.id', $this->quote('|%')) . ' OR c2.id = c.id)
            LEFT JOIN s_articles_categories_ro ro
            ON ro.categoryID = c2.id AND ro.articleID = ac.articleID
            WHERE ' . $sql . '
            AND ro.id IS NULL
            ORDER BY c2.id, c.id;
        ';
        $stmt = $this->connection->executeQuery(
            $sql,
            [
                ':articleId' => $articleId,
                ':categoryIds' => (array)$categoryIds
            ],
            [
                ':articleId' => \PDO::PARAM_INT,
                ':categoryIds' => Connection::PARAM_INT_ARRAY
            ]
        );
        return $stmt->rowCount();
    }

    /**
     * Adds new assignment between $articleId and $categoryId
     *
     * @param int $articleId
     * @param int|array $categoryIds
     * @return int
     */
    public function addAssignment($articleId, $categoryIds)
    {
        $sql = '
            INSERT INTO s_articles_categories_ro (articleID, categoryID, parentCategoryID)

            SELECT :articleId, c2.id AS categoryID, c.id AS parentCategoryID
            FROM s_categories c, s_categories c2
            LEFT JOIN s_articles_categories_ro ro
            ON ro.categoryID = c2.id AND ro.articleID = :articleId
            WHERE c.id IN (:categoryIds)
            AND (c.path LIKE ' . $this->concat($this->quote('%|'), 'c2.id', $this->quote('|%')) . ' OR c2.id = c.id)
            AND ro.id IS NULL
            ORDER BY c2.id, c.id;
        ';
        $stmt = $this->connection->executeQuery(
            $sql,
            [
                ':articleId' => $articleId,
                ':categoryIds' => (array)$categoryIds
            ],
            [
                ':articleId' => \PDO::PARAM_INT,
                ':categoryIds' => Connection::PARAM_INT_ARRAY
            ]
        );

        return $stmt->rowCount();
    }

    /**
     * Removes all connections for given $articleId
     *
     * @param int $articleId
     * @return int count of deleted rows
     */
    public function removeArticleAssignmentments($articleId)
    {
        $deleteQuery = '
            DELETE
            FROM s_articles_categories_ro
            WHERE articleID = :articleId
        ';

        $stmt = $this->connection->prepare($deleteQuery);
        $stmt->execute(array('articleId' => $articleId));

        return $stmt->rowCount();
    }

    /**
     * Removes all connections for given $categoryId
     *
     * @param  int $categoryId
     * @return int count of deleted rows
     */
    public function removeCategoryAssignmentments($categoryId)
    {
        return $this->removeOldAssignments($categoryId);
    }

    /**
     * First try to truncate table,
     * if that Fails due to insufficient permissions, use delete query
     *
     * @return int
     */
    public function removeAllAssignments()
    {
        // TRUNCATE is faster than DELETE
        try {
            $count = $this->connection->exec('TRUNCATE s_articles_categories_ro');
        } catch (\Doctrine\DBAL\DBALException $e) {
            $count = $this->connection->exec('DELETE FROM s_articles_categories_ro');
        }

        return $count;
    }

    /**
     * Removes assignments for non-existing articles or categories
     *
     * @return int
     */
    public function removeOrphanedAssignments()
    {
        $deleteOrphanedSql = '
            DELETE ac
            FROM s_articles_categories ac
            LEFT JOIN s_articles a ON ac.articleID = a.id
            WHERE a.id IS NULL
        ';
        $count = $this->connection->exec($deleteOrphanedSql);

        $deleteOrphanedSql = '
            DELETE ac
            FROM s_articles_categories ac
            LEFT JOIN s_categories c ON ac.categoryID = c.id
            WHERE c.id IS NULL
        ';
        $count += $this->connection->exec($deleteOrphanedSql);

        return $count;
    }

    /**
     * Adds an adapter-specific LIMIT clause to the SELECT statement.
     *
     * @param  string $sql
     * @param  integer $count
     * @param  integer $offset OPTIONAL
     * @throws \Exception
     * @return string
     */
    private function limit($sql, $count, $offset = 0)
    {
        return $this->connection->getDatabasePlatform()->modifyLimitQuery($sql, $count, $offset);
    }

    /**
     * Returns an adapter-specific CONCAT clause.
     *
     * @param $parts
     * @return string
     */
    private function concat(...$parts)
    {
        return $this->connection->getDatabasePlatform()->getConcatExpression(...$parts);
    }

    /**
     * Quotes a given input parameter.
     *
     * @param mixed $input
     * @return string|null
     */
    private function quote($input)
    {
        return $this->connection->quote($input);
    }

    /**
     * Wrapper around pdo::commit()
     */
    public function beginTransaction()
    {
        if ($this->transactionsEnabled()) {
            $this->connection->beginTransaction();
        }
    }

    /**
     * Wrapper around pdo::commit()
     */
    public function commit()
    {
        if ($this->transactionsEnabled()) {
            $this->connection->commit();
        }
    }
}
