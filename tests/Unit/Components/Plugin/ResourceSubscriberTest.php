<?php
namespace Shopware\Tests\Unit\Components\Plugin;

use PHPUnit\Framework\TestCase;
use Shopware\Components\Plugin\ResourceSubscriber;
use Shopware\Components\Theme\LessDefinition;

class ResourceSubscriberTest extends TestCase
{
    public function testEmptyPlugin()
    {
        $subscriber = new ResourceSubscriber(__DIR__.'/examples/EmptyPlugin');

        $this->assertNull($subscriber->onCollectCss());
        $this->assertNull($subscriber->onCollectJavascript());
        $this->assertNull($subscriber->onCollectLess());
    }
    public function testFoo()
    {
        $subscriber = new ResourceSubscriber(__DIR__.'/examples/TestPlugin');

        $this->assertSame(
            [
                __DIR__.'/examples/TestPlugin/Resources/frontend/css/foo/bar.css',
                __DIR__.'/examples/TestPlugin/Resources/frontend/css/test.css'
            ],
            $subscriber->onCollectCss()->toArray()
        );

        $this->assertSame(
            [
                __DIR__.'/examples/TestPlugin/Resources/frontend/js/foo.js',
                __DIR__.'/examples/TestPlugin/Resources/frontend/js/foo/bar.js'
            ],
            $subscriber->onCollectJavascript()->toArray()
        );

        $this->assertEquals(
            new LessDefinition([], [
                __DIR__.'/examples/TestPlugin/Resources/frontend/less/bar.less',
                __DIR__.'/examples/TestPlugin/Resources/frontend/less/foo/foo.less'
            ]),
            $subscriber->onCollectLess()
        );

    }
}
