import pytest

from xtrek.print_filename import short_operator


@pytest.mark.parametrize(('source', 'expected'), [
    ('Горбатов Артур Никитич', 'GorbaAN'),
    ('Любанин Яков Аркадьевич', 'LyubanYaA'),
    ('Кладовщик маркетплейса', 'KladoMa'),
    ('Оператор маркетплейса #3', 'OperaMa3'),
    ('Зам.нач.смены пр-во #1', 'ZamNaSmPr1'),
    ('Начальник смены (зона пр-ва #3)', 'NachalSmZoPr3'),
    ('Сотрудник КРГ', 'SotruKRG'),
    ('  Ёжова-Ли   Юлия   Яковлевна ', 'YozhovaYuYa'),
    ('Оператор / МП #12', 'OperaMP12'),
    (None, 'NA'), ('   ', 'NA'), ('---', 'NA'),
])
def test_short_operator(source, expected):
    assert short_operator(source) == expected
