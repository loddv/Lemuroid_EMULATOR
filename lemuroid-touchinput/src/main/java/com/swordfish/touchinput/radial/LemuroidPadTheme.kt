package com.swordfish.touchinput.radial

import androidx.compose.runtime.compositionLocalOf
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.DefaultShadowColor
import androidx.compose.ui.unit.Dp
import androidx.compose.ui.unit.dp

class LemuroidPadTheme(
    red: Float = 1f,
    green: Float = 1f,
    blue: Float = 1f,
    alpha: Float = 1f,
) {
    private val base = Color(red.coerceIn(0f, 1f), green.coerceIn(0f, 1f), blue.coerceIn(0f, 1f))
    private val inverse = Color(1f - base.red, 1f - base.green, 1f - base.blue)
    private val alphaMultiplier = alpha.coerceIn(0f, 1f)

    // luminosity 1 maps to the base color, 0 to its inverse (used for icons and pressed states)
    private fun gray(
        luminosity: Float,
        opacity: Float,
    ): Color {
        return (if (luminosity >= 0.5f) base else inverse).copy(alpha = opacity * alphaMultiplier)
    }

    val foregroundPadding: Dp = 8.dp
    val padding: Dp = 4.dp

    private val icons = gray(0.0f, 0.50f)
    private val iconsPressed = gray(1.0f, 0.50f)

    private val level3Fill = gray(1.0f, 0.50f)
    private val level3FillPressed = gray(0.0f, 0.50f)
    val level3Shadow = DefaultShadowColor.copy(0.05f)
    val level3ShadowWidth = 4.dp

    private val level2Fill = gray(1.0f, 0.125f)
    private val level2FillPressed = gray(0.0f, 0.125f)
    val level2Shadow = DefaultShadowColor.copy(0.05f)
    val level2ShadowWidth = 4.dp

    val level1Fill = gray(1.0f, 0.10f)
    val level1Shadow = DefaultShadowColor.copy(0.10f)
    val level1ShadowWidth = 4.dp

    val level0CornerRadius = 0.dp
    val level0Fill = gray(1.0f, 0.05f)
    val level0Shadow = DefaultShadowColor.copy(0.10f)
    val level0ShadowWidth = 2.dp

    fun compositeFill(pressed: Boolean): Color {
        return if (pressed) {
            level2FillPressed
        } else {
            level2Fill
        }
    }

    fun foregroundFill(pressed: Boolean): Color {
        return if (pressed) {
            level3FillPressed
        } else {
            level3Fill
        }
    }

    fun icons(pressed: Boolean): Color {
        return if (pressed) {
            iconsPressed
        } else {
            icons
        }
    }
}

val LocalLemuroidPadTheme =
    compositionLocalOf<LemuroidPadTheme> {
        error("LemuroidPadTheme is missing")
    }
